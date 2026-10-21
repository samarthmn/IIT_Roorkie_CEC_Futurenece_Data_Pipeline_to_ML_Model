"""
dags/nammamart_etl_samarth.py
-----------------------------
NammaMart ETL (Part A): Extract -> Validate -> Transform -> Load, in pandas.
Only rows that pass every Critical check reach the finance tables; failed rows go to
include/quarantine/samarth/<file>_failed_<run_id>.csv with a failure_reason.

Flow:
  extract_orders ─────┐
  extract_payments ───┤
  extract_customers ──┤
  extract_products ───┼──► validate ─► transform ─► load_star_schema ─► reconcile_payments ─► build_reports ─► audit_run
  extract_stores ─────┤   (+quarantine)
  extract_increments ─┘

audit_run (trigger_rule="all_done") always runs: it writes dq_results and pipeline_runs, then fails
the run if any earlier task failed.

Tasks that open DuckDB run in the one-slot pool "duckdb_samarth".
Intermediate DataFrames travel as pickle files under include/tmp/samarth/<run_id>/, not via XCom.
"""

import os
from datetime import timedelta

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

from include.nammamart import config

DAG_ID = "nammamart_etl_samarth"
IST = pendulum.timezone(config.TIMEZONE)

default_args = {
    "owner": "samarth",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=20),
}

PIPELINE_TASKS = [
    "extract_orders", "extract_payments", "extract_customers", "extract_products", "extract_stores",
    "extract_increments", "validate", "transform", "load_star_schema", "reconcile_payments", "build_reports",
]


def _run() -> dict:
    from include.nammamart.runtime import run_info
    return run_info(get_current_context())


def _tmp_path(run: dict, name: str) -> str:
    from include.nammamart.sources import ensure_dir
    return os.path.join(ensure_dir(os.path.join(config.TMP_DIR, run["safe_run_id"])), f"{name}.pkl")


def _extract_file(path, file_name: str, run: dict) -> dict:
    from include.nammamart.sources import check_header, read_source

    from include.nammamart import logs

    expected = config.EXPECTED_COLUMNS.get(file_name, config.ORDER_COLUMNS)
    extra = check_header(path, expected)
    df = read_source(path, expected)
    out = _tmp_path(run, f"extract_{path.stem}")
    df.to_pickle(out)
    logs.section(f"EXTRACT  {file_name}")
    logs.kv({"source": str(path), "header": f"OK ({len(expected)} expected columns present)",
             "rows read": logs.num(len(df))})
    if extra:
        print(f"  WARNING: unexpected extra columns ignored: {extra}")
    return {"file": file_name, "path": out, "rows": len(df)}


def _save_validation(run: dict, name: str, checked, quarantined, results) -> dict:
    """Pickle the checked rows and write one quarantine CSV per source file (even when empty)."""
    import pandas as pd
    from include.nammamart.sources import ensure_dir

    checked_path, quarantine_path = _tmp_path(run, f"checked_{name}"), _tmp_path(run, f"quarantined_{name}")
    checked.to_pickle(checked_path)
    quarantined.to_pickle(quarantine_path)

    files = []
    qdir = ensure_dir(config.QUARANTINE_DIR)
    for source_file in sorted(set(checked["_source_file"]) | set(quarantined["_source_file"])):
        cols = config.EXPECTED_COLUMNS.get(source_file, config.ORDER_COLUMNS)
        rows = quarantined[quarantined["_source_file"] == source_file]
        out = rows[cols].assign(failure_reason=rows["failure_reason"], source_row=rows["_source_row"],
                                run_id=run["run_id"])
        path = os.path.join(qdir, f"{source_file.removesuffix('.csv')}_failed_{run['safe_run_id']}.csv")
        out.sort_values("source_row").to_csv(path, index=False)
        files.append({"file": source_file, "path": path, "rows": len(out)})

    summary = {
        "file": name,
        "rows_read": len(checked) + int(quarantined["failure_reason"].str.startswith("duplicate_").sum()),
        "rows_passed": int((checked["dq_status"] != "FAIL").sum()),
        "rows_warned": int((checked["dq_status"] == "WARN").sum()),
        "rows_quarantined": len(quarantined),
    }
    return {**summary, "checked_path": checked_path, "quarantined_path": quarantine_path,
            "quarantine_files": files, "results": results}


def _log_validation(v: dict) -> None:
    from include.nammamart import logs

    logs.section("VALIDATE  rows read, passed and quarantined per file")
    rows = [{"file": f, "read": v[f]["rows_read"], "passed": v[f]["rows_passed"],
             "of which warnings": v[f]["rows_warned"], "quarantined": v[f]["rows_quarantined"]}
            for f in ["orders", "payments", "customers", "products", "stores"]]
    rows.append({"file": "TOTAL", **{k: sum(r[k] for r in rows) for k in rows[0] if k != "file"}})
    logs.table([{k: (logs.num(val) if k != "file" else val) for k, val in r.items()} for r in rows])
    print("  passed = loads to the warehouse (warning rows included); quarantined = set aside with a reason")

    for f in ["orders", "payments", "customers", "products", "stores"]:
        problems = [r for r in v[f]["results"] if r["status"] != "PASS"]
        logs.subsection(f"{f}: {len(problems)} of {len(v[f]['results'])} checks found problems")
        logs.table([{"status": r["status"], "check": r["check_name"], "dimension": r["dimension"],
                     "rows failed": f"{r['rows_failed']:,} / {r['rows_checked']:,}"} for r in problems])

    orders = v["orders"]
    if orders["merge_stats"]:
        logs.subsection("incremental files (upsert on order_id)")
        logs.table([{"file": s["file"], "rows": s["rows"], "inserted": s["inserted"], "updated": s["updated"],
                     "rejected": s["rejected"]} for s in orders["merge_stats"]])
    fresh = orders["freshness"]
    logs.subsection("freshness")
    logs.kv({"latest valid order": fresh["latest_order_ts"] or "none",
             "reference time (IST)": fresh["as_of"],
             "lag": f"{fresh['lag_hours']} h (limit {config.FRESHNESS_MAX_LAG_HOURS} h)",
             "result": "WARN: data is stale" if fresh["stale"] else "PASS"})

    logs.subsection(f"quarantine files in {config.QUARANTINE_DIR}")
    logs.table([{"rows": q["rows"], "file": os.path.basename(q["path"])}
                for f in v.values() for q in f["quarantine_files"]])


@dag(
    dag_id=DAG_ID,
    schedule="0 9 * * 1-6",                                # 09:00 Monday–Saturday, in the start_date timezone
    start_date=pendulum.datetime(2026, 10, 1, tz=IST),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=25),
    default_args=default_args,
    params={
        "data_dir": Param(config.DEFAULT_DATA_DIR, type="string",
                          description="Input folder, relative to AIRFLOW_HOME (must be under include/data)"),
        "as_of_date": Param(None, type=["null", "string"],
                            description="Freshness reference date YYYY-MM-DD (09:00 IST); default = logical date"),
    },
    tags=["nammamart", "etl", "samarth"],
)
def nammamart_etl_samarth():

    # ── EXTRACT (five parallel tasks + optional increments) ───────────────────

    @task
    def extract(file_name: str) -> dict:
        from include.nammamart.sources import resolve_data_dir
        ctx = get_current_context()
        return _extract_file(resolve_data_dir(ctx["params"]["data_dir"]) / file_name, file_name, _run())

    @task
    def extract_increments() -> list:
        from include.nammamart.sources import list_increments, resolve_data_dir
        run = _run()
        paths = list_increments(resolve_data_dir(get_current_context()["params"]["data_dir"]))
        if not paths:
            from include.nammamart import logs
            logs.section("EXTRACT  increments")
            print(f"  No {config.INCREMENT_GLOB} file in the data folder; nothing to add.")
        return [_extract_file(p, p.name, run) for p in paths]

    # ── VALIDATE + QUARANTINE ─────────────────────────────────────────────────

    @task
    def validate(orders_x: dict, payments_x: dict, customers_x: dict, products_x: dict, stores_x: dict,
                 increments_x: list) -> dict:
        """Every Part D rule, file by file: masters first, then orders (needs the master keys),
        then payments (needs the orders). Writes the quarantine files; returns one summary per file."""
        import pandas as pd
        from include.nammamart import quality

        run = _run()
        raw = {name: pd.read_pickle(x["path"]) for name, x in
               [("orders", orders_x), ("payments", payments_x), ("customers", customers_x),
                ("products", products_x), ("stores", stores_x)]}
        increments = [pd.read_pickle(i["path"]) for i in increments_x]

        out = {
            "stores": _save_validation(run, "stores", *quality.validate_stores(raw["stores"])),
            "products": _save_validation(run, "products", *quality.validate_products(raw["products"])),
            "customers": _save_validation(run, "customers", *quality.validate_customers(raw["customers"], run)),
        }

        keys = {}
        for name, key in [("customers", "customer_id"), ("products", "product_id"), ("stores", "store_id")]:
            k = raw[name][key].str.strip()
            keys[name] = set(k[k.ne("")])
        checked, quarantined, results, merge_stats, freshness = quality.validate_orders(
            raw["orders"], increments, keys, run)
        for s in merge_stats:
            s["rejected"] = int((quarantined["_source_file"] == s["file"]).sum())
        orders_v = _save_validation(run, "orders", checked, quarantined, results)
        orders_v["rows_read"] = orders_x["rows"] + sum(i["rows"] for i in increments_x)
        out["orders"] = {**orders_v, "merge_stats": merge_stats, "freshness": freshness,
                         "rows_canonical": len(checked)}

        ids = pd.concat([raw["orders"]["order_id"], *[i["order_id"] for i in increments]]).str.strip()
        source_order_ids = set(ids[ids.ne("")])
        out["payments"] = _save_validation(run, "payments", *quality.validate_payments(
            raw["payments"], source_order_ids, checked, run))
        out["orders"]["source_ids_path"] = _tmp_path(run, "source_order_ids")
        pd.Series(sorted(source_order_ids)).to_pickle(out["orders"]["source_ids_path"])
        _log_validation(out)
        return out

    # ── TRANSFORM ─────────────────────────────────────────────────────────────

    @task
    def transform(v: dict) -> dict:
        import pandas as pd
        from include.nammamart import etl

        run = _run()
        dim_store = etl.build_dim_store(pd.read_pickle(v["stores"]["checked_path"]))
        dim_product = etl.build_dim_product(pd.read_pickle(v["products"]["checked_path"]))
        dim_customer = etl.build_dim_customer(pd.read_pickle(v["customers"]["checked_path"]))
        orders = pd.read_pickle(v["orders"]["checked_path"])
        orders = orders[orders["dq_status"] != "FAIL"]

        for col, dim, key in [("customer_id", dim_customer, "customer_id"), ("product_id", dim_product, "product_id"),
                              ("store_id", dim_store, "store_id")]:
            missing = set(orders[col].str.strip()) - set(dim[key])
            if missing:
                raise ValueError(f"{len(missing)} {col} values have no dimension row: {sorted(missing)[:10]}")
        fact = etl.build_fact(orders, dim_product, run)

        from include.nammamart import logs

        tables = {}
        for name, df in [("dim_store", dim_store), ("dim_product", dim_product), ("dim_customer", dim_customer),
                         ("fact_orders", fact)]:
            tables[name] = _tmp_path(run, name)
            df.to_pickle(tables[name])
        logs.section("TRANSFORM  standardise, dedupe, mask PII, join, derive")
        logs.table([
            {"table": "fact_orders", "rows": logs.num(len(fact)), "note": "orders that passed every Critical check"},
            {"table": "dim_customer", "rows": logs.num(len(dim_customer)), "note": "email and phone masked; no name"},
            {"table": "dim_product", "rows": logs.num(len(dim_product)), "note": "blank category shown as Unknown"},
            {"table": "dim_store", "rows": logs.num(len(dim_store)), "note": "blank zone shown as Unknown"},
        ])
        delivered = fact[fact["order_status"] == "DELIVERED"]
        logs.kv({"delivered orders": logs.num(len(delivered)),
                 "net revenue": logs.money(delivered["net_revenue"].sum()),
                 "gross margin": logs.money(delivered["gross_margin"].sum())})
        return {"validation": v, "tables": tables}

    # ── LOAD (idempotent: the whole star is replaced in one transaction) ──────

    @task(pool=config.POOL)
    def load_star_schema(t: dict) -> dict:
        import pandas as pd
        from include.nammamart import warehouse

        run = _run()
        frames = {name: pd.read_pickle(path) for name, path in t["tables"].items()}
        rejected = []
        for file_v in t["validation"].values():
            for q in file_v["quarantine_files"]:
                df = pd.read_csv(q["path"], dtype=str, keep_default_na=False)
                key = config.PRIMARY_KEYS.get(q["file"], "order_id")
                rejected.append(pd.DataFrame({"file_name": q["file"], "row_key": df[key],
                                              "failure_reason": df["failure_reason"]}))
        rejected = pd.concat(rejected, ignore_index=True)

        with warehouse.connect() as con:
            for name, df in {**frames, "rejected": rejected}.items():
                con.register(f"df_{name}", df)
            con.begin()
            try:
                con.execute("""
                    CREATE OR REPLACE TABLE etl_dim_store AS
                    SELECT store_id, store_name, zone, area, pincode, CAST(open_date AS DATE) AS open_date,
                           CAST(capacity_orders_per_day AS INTEGER) AS capacity_orders_per_day,
                           dq_status, failure_reason, warning_reason
                    FROM df_dim_store;

                    CREATE OR REPLACE TABLE etl_dim_product AS
                    SELECT product_id, product_name, category, sub_category, brand,
                           CAST(mrp AS DECIMAL(12,2)) AS mrp, CAST(cost_price AS DECIMAL(12,2)) AS cost_price,
                           unit, is_perishable, CAST(launch_date AS DATE) AS launch_date,
                           dq_status, failure_reason, warning_reason
                    FROM df_dim_product;

                    CREATE OR REPLACE TABLE etl_dim_customer AS
                    SELECT customer_id, email_masked, phone_masked, city, area, loyalty_tier,
                           CAST(signup_date AS DATE) AS signup_date, is_active,
                           dq_status, failure_reason, warning_reason
                    FROM df_dim_customer;

                    CREATE OR REPLACE TABLE etl_fact_orders AS
                    SELECT order_id, order_ts, CAST(order_date AS DATE) AS order_date, customer_id, store_id,
                           product_id, CAST(quantity AS INTEGER) AS quantity,
                           CAST(unit_price AS DECIMAL(12,2)) AS unit_price,
                           CAST(discount_pct AS DECIMAL(5,2)) AS discount_pct,
                           CAST(order_amount AS DECIMAL(12,2)) AS order_amount,
                           payment_mode, order_status, channel,
                           CAST(delivery_minutes AS INTEGER) AS delivery_minutes,
                           CAST(net_revenue AS DECIMAL(12,2)) AS net_revenue,
                           CAST(gross_margin AS DECIMAL(12,2)) AS gross_margin,
                           within_15_min, source_file, CAST(source_row AS INTEGER) AS source_row, run_id,
                           CAST(now() AS TIMESTAMP) AS loaded_at
                    FROM df_fact_orders;

                    CREATE TABLE IF NOT EXISTS etl_rejected_rows (
                        run_id VARCHAR, dag_id VARCHAR, run_date_ist DATE, file_name VARCHAR,
                        row_key VARCHAR, failure_reason VARCHAR, rejected_at TIMESTAMP
                    );
                """)
                con.execute("DELETE FROM etl_rejected_rows WHERE run_id = ? AND dag_id = ?",
                            [run["run_id"], run["dag_id"]])
                con.execute("""
                    INSERT INTO etl_rejected_rows
                    SELECT ?, ?, CAST(? AS DATE), file_name, row_key, failure_reason, CAST(now() AS TIMESTAMP)
                    FROM df_rejected
                """, [run["run_id"], run["dag_id"], run["run_date_ist"]])
                con.commit()
            except Exception:
                con.rollback()
                raise
            dupes = con.execute("SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM etl_fact_orders").fetchone()[0]
            counts = {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                      for t in ["etl_fact_orders", "etl_dim_customer", "etl_dim_product", "etl_dim_store"]}
        if dupes:
            raise ValueError(f"etl_fact_orders has {dupes} duplicate order_id rows")
        from include.nammamart import logs
        logs.section(f"LOAD  {os.path.basename(config.WAREHOUSE_DB)}  (one transaction, tables replaced)")
        logs.table([{"table": name, "rows": logs.num(n)} for name, n in counts.items()]
                   + [{"table": "etl_rejected_rows (+ this run)", "rows": logs.num(len(rejected))}])
        print("  check: order_id is unique in etl_fact_orders")
        return {**t, "rows_loaded": counts["etl_fact_orders"]}

    # ── RECONCILE + REPORT ────────────────────────────────────────────────────

    @task(pool=config.POOL)
    def reconcile_payments(loaded: dict) -> dict:
        import pandas as pd
        from include.nammamart import etl, warehouse

        orders_v, payments_v = loaded["validation"]["orders"], loaded["validation"]["payments"]
        quarantined = pd.read_pickle(payments_v["quarantined_path"])
        recon = etl.reconcile(
            fact=pd.read_pickle(loaded["tables"]["fact_orders"]),
            orders_checked=pd.read_pickle(orders_v["checked_path"]),
            payments_checked=pd.read_pickle(payments_v["checked_path"]),
            payment_extras=quarantined[quarantined["failure_reason"] == "duplicate_payment_id"],
            source_order_ids=set(pd.read_pickle(orders_v["source_ids_path"])),
        )
        recon["run_id"] = _run()["run_id"]
        with warehouse.connect() as con:
            con.register("df_recon", recon)
            con.execute("""
                CREATE OR REPLACE TABLE etl_payment_reconciliation AS
                SELECT order_id, payment_id, order_status, CAST(order_amount AS DECIMAL(12,2)) AS order_amount,
                       CAST(amount_paid AS DECIMAL(12,2)) AS amount_paid,
                       CAST(difference AS DECIMAL(12,2)) AS difference, reason, run_id
                FROM df_recon
            """)
            summary = con.sql("""
                SELECT COALESCE(reason, 'TOTAL') AS reason, COUNT(*) AS n,
                       SUM(difference) AS signed_diff, SUM(ABS(difference)) AS abs_diff
                FROM etl_payment_reconciliation GROUP BY ROLLUP (reason) ORDER BY reason NULLS LAST
            """).df()
        from include.nammamart import logs
        logs.section("RECONCILE  orders vs payments.csv  ->  etl_payment_reconciliation")
        logs.table([{"reason": r.reason, "rows": logs.num(r.n), "difference (signed)": logs.money(r.signed_diff),
                     "difference (absolute)": logs.money(r.abs_diff)} for r in summary.itertuples()])
        print("  difference = SUCCESS amount paid - amount owed (order_amount if DELIVERED, else 0)")
        return {"reconciliation_rows": len(recon)}

    @task(pool=config.POOL)
    def build_reports(_reconciled: dict) -> dict:
        import pandas as pd
        from include.nammamart import warehouse

        with warehouse.connect() as con:
            con.execute("""
                CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
                SELECT f.order_date, s.zone, f.store_id, s.store_name,
                       COUNT(*) AS delivered_orders, SUM(f.net_revenue) AS net_revenue
                FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
                WHERE f.order_status = 'DELIVERED'
                GROUP BY ALL ORDER BY f.order_date, s.zone, f.store_id;

                CREATE OR REPLACE TABLE etl_category_margin AS
                SELECT p.category, COUNT(*) AS delivered_orders, SUM(f.net_revenue) AS revenue,
                       SUM(f.quantity * p.cost_price) AS cost, SUM(f.gross_margin) AS gross_margin,
                       ROUND(100 * SUM(f.gross_margin) / NULLIF(SUM(f.net_revenue), 0), 2) AS margin_pct
                FROM etl_fact_orders f JOIN etl_dim_product p USING (product_id)
                WHERE f.order_status = 'DELIVERED'
                GROUP BY ALL ORDER BY revenue DESC;
            """)
            zone = con.sql("SELECT * FROM etl_daily_zone_revenue").df()
            category = con.sql("SELECT * FROM etl_category_margin").df()
        from include.nammamart import logs
        logs.section("REPORT  etl_category_margin  (DELIVERED orders)")
        logs.table([{"category": r.category, "orders": logs.num(r.delivered_orders), "revenue": logs.money(r.revenue),
                     "cost": logs.money(r.cost), "gross margin": logs.money(r.gross_margin),
                     "margin %": f"{r.margin_pct:.2f}%"} for r in category.itertuples()])
        logs.kv({"total revenue": logs.money(category["revenue"].sum()),
                 "total gross margin": logs.money(category["gross_margin"].sum())})

        logs.section(f"REPORT  etl_daily_zone_revenue  ({len(zone):,} rows: date x zone x store)")
        by_zone = zone.groupby("zone", as_index=False)[["delivered_orders", "net_revenue"]].sum()
        logs.subsection("totals by zone")
        logs.table([{"zone": r.zone, "orders": logs.num(r.delivered_orders), "net revenue": logs.money(r.net_revenue)}
                    for r in by_zone.itertuples()])
        logs.kv({"total delivered orders": logs.num(zone["delivered_orders"].sum()),
                 "total net revenue": logs.money(zone["net_revenue"].sum())})
        logs.subsection("full table")
        logs.table([{"date": str(r.order_date), "zone": r.zone, "store": r.store_id, "store name": r.store_name,
                     "orders": logs.num(r.delivered_orders), "net revenue": logs.money(r.net_revenue)}
                    for r in zone.itertuples()])
        return {"zone_rows": len(zone), "category_rows": len(category)}

    # ── RUN AUDIT (always runs; keeps a failed run red) ───────────────────────

    @task(pool=config.POOL, trigger_rule="all_done")
    def audit_run() -> None:
        """Writes this run's quality scorecard (dq_results) and its row in pipeline_runs."""
        import shutil
        from airflow.sdk.exceptions import AirflowFailException
        from include.nammamart import logs, quality, warehouse

        ctx = get_current_context()
        ti, run = ctx["ti"], _run()
        outputs = {t: ti.xcom_pull(task_ids=t) for t in PIPELINE_TASKS}
        failed = [t for t, v in outputs.items() if v is None]

        extracts = [outputs[t] for t in PIPELINE_TASKS[:5] if outputs[t]] + (outputs["extract_increments"] or [])
        validations = outputs["validate"] or {}
        results = [r for file_v in validations.values() for r in file_v["results"]]
        row = {
            "run_id": run["run_id"], "dag_id": run["dag_id"],
            "start_time": pendulum.instance(ctx["dag_run"].start_date).in_timezone(IST).naive(),
            "end_time": pendulum.now(IST).naive(),
            "rows_extracted": sum(x["rows"] for x in extracts),
            "rows_loaded": (outputs["load_star_schema"] or {}).get("rows_loaded", 0),
            "rows_quarantined": sum(v["rows_quarantined"] for v in validations.values()),
            "status": "FAILED" if failed else "SUCCESS",
        }
        write_error = None
        try:
            with warehouse.connect() as con:
                warehouse.ensure_control_tables(con)
                if results:                            # validation ran: record every check
                    orders = validations["orders"]["rows_canonical"]
                    previous = warehouse.previous_rows_checked(con, run["dag_id"], run["run_id"],
                                                               "orders_required_fields")
                    results.append(quality.volume_check(orders, previous))
                    warehouse.write_dq_results(con, run, results)
                    summary = {s: sum(r["status"] == s for r in results) for s in ["PASS", "WARN", "FAIL"]}
                    if not failed and (summary["WARN"] or summary["FAIL"]):
                        row["status"] = "SUCCESS_WITH_WARNINGS"
                    logs.section("AUDIT  dq_results (quality scorecard)")
                    logs.kv({"checks written": f"{len(results)}  (PASS {summary['PASS']}, WARN {summary['WARN']}, "
                                               f"FAIL {summary['FAIL']})",
                             "volume check": f"{orders:,} orders this run vs "
                                             f"{'no previous run' if previous is None else f'{previous:,} last run'}"
                                             f" -> {results[-1]['status']}"})
                    print("  FAIL = rows were quarantined; WARN = a Warning check found rows")
                warehouse.upsert_pipeline_run(con, row)
        except Exception as exc:                       # never hide the original failure
            write_error = exc
            print(f"  could not write to the warehouse: {exc!r}")
        logs.section("AUDIT  pipeline_runs")
        logs.kv({k: logs.num(v) if k.startswith("rows_") else str(v)[:19] if k.endswith("_time") else v
                 for k, v in row.items()})
        if failed:
            print(f"  tasks without a result: {', '.join(failed)}")
            raise AirflowFailException(f"Run failed; tasks without a result: {failed}")
        if write_error:
            raise write_error
        shutil.rmtree(os.path.join(config.TMP_DIR, run["safe_run_id"]), ignore_errors=True)

    # ── WIRING ────────────────────────────────────────────────────────────────

    x = {f: extract.override(task_id=f"extract_{f.removesuffix('.csv')}")(f) for f in config.BASE_FILES}
    increments = extract_increments()

    v = validate(x["orders.csv"], x["payments.csv"], x["customers.csv"], x["products.csv"], x["stores.csv"],
                 increments)
    # A straight line: a failure anywhere marks every later task upstream_failed,
    # so audit_run (all_done) only needs to follow the last one.
    build_reports(reconcile_payments(load_star_schema(transform(v)))) >> audit_run()


nammamart_etl_samarth()
