"""
dags/nammamart_elt_samarth.py
-----------------------------
NammaMart ELT (Part B): Extract -> Load raw -> Transform in the warehouse with SQL.
Every raw row is kept exactly as received; all cleaning happens in SQL (include/sql/*.sql).

Flow:
  check_orders ───────┐
  check_payments ─────┤
  check_customers ────┤
  check_products ─────┼──► load_raw ─► transform_staging ─► transform_marts ─► audit_run
  check_stores ───────┤
  check_increments ───┘

  check_*            Extract: file present + header as expected (no DB access, so they run in parallel)
  load_raw           Load: raw_* tables, an exact copy of every file plus _loaded_at, _source_file, _run_id
  transform_staging  Transform (SQL): stg_* tables: cast, standardise, dedupe, mask PII, MERGE increments,
                     quality rules, dq_status / failure_reason; writes dq_results
  transform_marts    Transform (SQL): mart_* tables answering the six business questions
  audit_run          pipeline_runs row (trigger_rule="all_done"); fails the run if any task failed

Tasks that open DuckDB run in the one-slot pool "duckdb_samarth".
"""

from datetime import timedelta

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

from include.nammamart import config

DAG_ID = "nammamart_elt_samarth"
IST = pendulum.timezone(config.TIMEZONE)

default_args = {
    "owner": "samarth",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=20),
}

PIPELINE_TASKS = [
    "check_orders", "check_payments", "check_customers", "check_products", "check_stores",
    "check_increments", "load_raw", "transform_staging", "transform_marts",
]
STAGING_FILES = ["20_stg_rejected_duplicates.sql", "21_stg_stores.sql", "22_stg_products.sql",
                 "23_stg_customers.sql", "24_stg_orders_build.sql"]
MART_FILES = ["40_mart_daily_store_kpis.sql", "41_mart_category_margin.sql", "42_mart_delivery_sla.sql",
              "43_mart_payment_reconciliation.sql", "44_mart_top_customers.sql"]


def _run() -> dict:
    from include.nammamart.runtime import run_info
    return run_info(get_current_context())


def _check_file(path, file_name: str) -> dict:
    from include.nammamart import logs
    from include.nammamart.sources import check_header, count_data_rows

    expected = config.EXPECTED_COLUMNS.get(file_name, config.ORDER_COLUMNS)
    extra = check_header(path, expected)
    rows = count_data_rows(path)
    logs.section(f"CHECK  {file_name}")
    logs.kv({"source": str(path), "header": f"OK ({len(expected)} expected columns present)",
             "data rows": logs.num(rows)})
    if extra:
        print(f"  WARNING: unexpected extra columns (not loaded): {extra}")
    return {"file": file_name, "path": str(path), "rows": rows}


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
    tags=["nammamart", "elt", "samarth"],
)
def nammamart_elt_samarth():

    # ── EXTRACT: check each file (parallel, no warehouse access) ──────────────

    @task
    def check(file_name: str) -> dict:
        from include.nammamart.sources import resolve_data_dir
        return _check_file(resolve_data_dir(get_current_context()["params"]["data_dir"]) / file_name, file_name)

    @task
    def check_increments() -> list:
        from include.nammamart import logs
        from include.nammamart.sources import list_increments, resolve_data_dir

        paths = list_increments(resolve_data_dir(get_current_context()["params"]["data_dir"]))
        if not paths:
            logs.section("CHECK  increments")
            print(f"  No {config.INCREMENT_GLOB} file in the data folder; nothing to add.")
        return [_check_file(p, p.name) for p in paths]

    # ── LOAD RAW: exact copy of every file ────────────────────────────────────

    @task(pool=config.POOL)
    def load_raw(*checks) -> dict:
        from include.nammamart import elt, logs, warehouse

        run = _run()
        files = []                                         # five base files, then the increments list
        for c in checks:
            files.extend(c if isinstance(c, list) else [c])
        loaded = []
        with warehouse.connect() as con:
            warehouse.run_sql_file(con, "10_raw_create.sql")
            for f in files:
                rows = elt.load_raw_file(con, f["path"], f["file"], run)
                loaded.append({"file": f["file"], "table": elt.raw_table(f["file"]), "rows": rows})
            batches = con.execute("SELECT COUNT(DISTINCT _run_id) FROM raw_orders").fetchone()[0]

        logs.section(f"LOAD RAW  run {run['run_id']}")
        logs.table([{"file": x["file"], "raw table": x["table"], "rows inserted": logs.num(x["rows"])}
                    for x in loaded])
        logs.kv({"rows inserted this run": logs.num(sum(x["rows"] for x in loaded)),
                 "check": "rows inserted = data rows in each file",
                 "raw_orders batches kept": f"{batches} (raw is append-only: one batch per run)"})
        return {"files": loaded, "rows_loaded": sum(x["rows"] for x in loaded)}

    # ── STAGING: SQL only ─────────────────────────────────────────────────────

    @task(pool=config.POOL)
    def transform_staging(raw: dict) -> dict:
        from include.nammamart import elt, logs, warehouse

        run = _run()
        increments = sorted(f["file"] for f in raw["files"] if f["file"].startswith("orders_"))
        with warehouse.connect() as con:
            warehouse.ensure_control_tables(con)
            warehouse.set_run_ctx(con, run)
            for name in STAGING_FILES:
                warehouse.run_sql_file(con, name)
            merges = []
            for file_name in increments:                       # oldest first (file names sort by date)
                con.execute("CREATE OR REPLACE TEMP TABLE merge_ctx AS SELECT ? AS source_file", [file_name])
                merges.append(elt.merge_counts(con, file_name))
                warehouse.run_sql_file(con, "25_stg_orders_merge_increment.sql")
            for name in ["26_stg_orders_apply_dq.sql", "27_stg_payments.sql", "30_dq_results_elt.sql"]:
                warehouse.run_sql_file(con, name)

            status = con.sql("""
                SELECT t AS "table", COUNT(*) AS rows,
                       COUNT(*) FILTER (WHERE dq_status = 'PASS') AS pass,
                       COUNT(*) FILTER (WHERE dq_status = 'WARN') AS warn,
                       COUNT(*) FILTER (WHERE dq_status = 'FAIL') AS fail
                FROM (SELECT 'stg_orders' AS t, dq_status FROM stg_orders
                      UNION ALL SELECT 'stg_payments', dq_status FROM stg_payments
                      UNION ALL SELECT 'stg_customers', dq_status FROM stg_customers
                      UNION ALL SELECT 'stg_products', dq_status FROM stg_products
                      UNION ALL SELECT 'stg_stores', dq_status FROM stg_stores)
                GROUP BY t ORDER BY rows DESC
            """).df()
            dups = con.sql("""SELECT file_name, COUNT(*) AS n FROM stg_rejected_duplicates
                              GROUP BY 1 ORDER BY 1""").df()
            dq = con.execute("""
                SELECT check_name, dimension, status, rows_failed, rows_checked FROM dq_results
                WHERE run_id = ? AND dag_id = ? ORDER BY check_name
            """, [run["run_id"], run["dag_id"]]).df()
            fresh = con.execute("""
                SELECT MAX(order_ts), (SELECT as_of_ts FROM run_ctx) FROM stg_orders WHERE dq_status <> 'FAIL'
            """).fetchone()
            pii = con.execute("""
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_name LIKE 'stg_%' AND column_name IN ('email', 'phone', 'full_name')
            """).fetchone()[0]
            dupe_ids = con.execute("SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM stg_orders").fetchone()[0]
        if dupe_ids:
            raise ValueError(f"stg_orders has {dupe_ids} duplicate order_id rows after the MERGE")
        if pii:
            raise ValueError("a staging table exposes a clear email, phone or full_name column")

        logs.section("STAGING  cast, standardise, dedupe, mask PII, apply quality rules")
        logs.table([{"table": r.table, "rows": logs.num(r.rows), "PASS": logs.num(r.pass_),
                     "WARN": logs.num(r.warn), "FAIL (set aside)": logs.num(r.fail)}
                    for r in status.rename(columns={"pass": "pass_"}).itertuples()])
        logs.subsection("extra duplicate copies (stg_rejected_duplicates)")
        logs.table([{"file": r.file_name, "rows": logs.num(r.n)} for r in dups.itertuples()])
        if merges:
            logs.subsection("MERGE increments into stg_orders (upsert on order_id)")
            logs.table(merges)
        logs.subsection("freshness")
        lag = None if fresh[0] is None else round((fresh[1] - fresh[0]).total_seconds() / 3600, 1)
        logs.kv({"latest valid order": fresh[0], "reference time (IST)": fresh[1],
                 "lag": f"{lag} h (limit {config.FRESHNESS_MAX_LAG_HOURS} h)",
                 "result": "PASS" if lag is not None and lag <= config.FRESHNESS_MAX_LAG_HOURS else "WARN: data is stale"})
        problems = dq[dq["status"] != "PASS"]
        logs.section(f"DQ_RESULTS  {len(dq)} checks written ({(dq['status'] == 'PASS').sum()} PASS, "
                     f"{(dq['status'] == 'WARN').sum()} WARN, {(dq['status'] == 'FAIL').sum()} FAIL)")
        logs.table([{"status": r.status, "check": r.check_name, "dimension": r.dimension,
                     "rows failed": f"{r.rows_failed:,} / {r.rows_checked:,}"} for r in problems.itertuples()])
        print("  FAIL = rows set aside in staging (dq_status = 'FAIL'); WARN = a Warning check found rows")

        return {
            **raw,
            "merges": merges,
            "rows_quarantined": int(status["fail"].sum() + dups["n"].sum()),
            "dq": {s: int((dq["status"] == s).sum()) for s in ["PASS", "WARN", "FAIL"]},
        }

    # ── MARTS: SQL only ───────────────────────────────────────────────────────

    @task(pool=config.POOL)
    def transform_marts(staged: dict) -> dict:
        from include.nammamart import logs, warehouse

        run = _run()
        with warehouse.connect() as con:
            warehouse.set_run_ctx(con, run)
            for name in MART_FILES:
                warehouse.run_sql_file(con, name)
            counts = {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                      for t in [f.removesuffix(".sql")[3:] for f in MART_FILES]}
            kpis = con.sql("""SELECT zone, SUM(delivered_orders) AS orders, SUM(net_revenue) AS revenue
                              FROM mart_daily_store_kpis GROUP BY zone ORDER BY zone""").df()
            category = con.sql("SELECT * FROM mart_category_margin").df()
            sla = con.sql("SELECT * FROM mart_delivery_sla").df()
            recon = con.sql("""
                SELECT COALESCE(reason, 'TOTAL') AS reason, COUNT(*) AS n,
                       SUM(difference) AS signed_diff, SUM(ABS(difference)) AS abs_diff
                FROM mart_payment_reconciliation GROUP BY ROLLUP (reason) ORDER BY reason NULLS LAST
            """).df()
            top = con.sql("SELECT * FROM mart_top_customers WHERE spend_rank <= 10").df()

        logs.section("MARTS  built from staging rows with dq_status PASS or WARN")
        logs.table([{"mart": t, "rows": logs.num(n)} for t, n in counts.items()])

        logs.section("mart_daily_store_kpis  (totals by zone, DELIVERED orders)")
        logs.table([{"zone": r.zone, "orders": logs.num(r.orders), "net revenue": logs.money(r.revenue)}
                    for r in kpis.itertuples()])
        logs.kv({"total delivered orders": logs.num(kpis["orders"].sum()),
                 "total net revenue": logs.money(kpis["revenue"].sum())})

        logs.section("mart_category_margin")
        logs.table([{"category": r.category, "orders": logs.num(r.delivered_orders), "revenue": logs.money(r.revenue),
                     "cost": logs.money(r.cost), "gross margin": logs.money(r.gross_margin),
                     "margin %": f"{r.margin_pct:.2f}%"} for r in category.itertuples()])
        logs.kv({"total gross margin": logs.money(category["gross_margin"].sum())})

        logs.section("mart_delivery_sla")
        logs.table([{"store": r.store_id, "name": r.store_name, "zone": r.zone, "delivered": logs.num(r.delivered_orders),
                     "avg minutes": f"{r.avg_delivery_minutes:.1f}", "within 15 min": f"{r.pct_within_15_min:.1f}%"}
                    for r in sla.itertuples()])

        logs.section("mart_payment_reconciliation")
        logs.table([{"reason": r.reason, "rows": logs.num(r.n), "difference (signed)": logs.money(r.signed_diff),
                     "difference (absolute)": logs.money(r.abs_diff)} for r in recon.itertuples()])
        print("  difference = SUCCESS amount paid - amount owed (order_amount if DELIVERED, else 0)")

        logs.section("mart_top_customers  (top 10; masked email only)")
        logs.table([{"rank": r.spend_rank, "customer": r.customer_id, "email (masked)": r.email_masked or "",
                     "tier": r.loyalty_tier, "orders": logs.num(r.delivered_orders), "spend": logs.money(r.spend)}
                    for r in top.itertuples()])
        return {**staged, "marts": counts}

    # ── RUN AUDIT (always runs; keeps a failed run red) ───────────────────────

    @task(pool=config.POOL, trigger_rule="all_done")
    def audit_run() -> None:
        from airflow.sdk.exceptions import AirflowFailException
        from include.nammamart import logs, warehouse

        ctx = get_current_context()
        ti, run = ctx["ti"], _run()
        outputs = {t: ti.xcom_pull(task_ids=t) for t in PIPELINE_TASKS}
        failed = [t for t, v in outputs.items() if v is None]

        checks = [outputs[t] for t in PIPELINE_TASKS[:5] if outputs[t]] + (outputs["check_increments"] or [])
        staged = outputs["transform_staging"] or {}
        dq = staged.get("dq", {})
        row = {
            "run_id": run["run_id"], "dag_id": run["dag_id"],
            "start_time": pendulum.instance(ctx["dag_run"].start_date).in_timezone(IST).naive(),
            "end_time": pendulum.now(IST).naive(),
            "rows_extracted": sum(c["rows"] for c in checks),
            "rows_loaded": (outputs["load_raw"] or {}).get("rows_loaded", 0),
            "rows_quarantined": staged.get("rows_quarantined", 0),
            "status": "FAILED" if failed else
                      "SUCCESS_WITH_WARNINGS" if dq.get("WARN") or dq.get("FAIL") else "SUCCESS",
        }
        write_error = None
        try:
            with warehouse.connect() as con:
                warehouse.ensure_control_tables(con)
                warehouse.upsert_pipeline_run(con, row)
        except Exception as exc:                       # never hide the original failure
            write_error = exc
            print(f"  could not write to the warehouse: {exc!r}")
        logs.section("AUDIT  pipeline_runs")
        logs.kv({k: logs.num(v) if k.startswith("rows_") else str(v)[:19] if k.endswith("_time") else v
                 for k, v in row.items()})
        print("  rows_loaded = rows inserted into raw_*; rows_quarantined = staging FAIL rows + extra duplicate copies")
        if failed:
            print(f"  tasks without a result: {', '.join(failed)}")
            raise AirflowFailException(f"Run failed; tasks without a result: {failed}")
        if write_error:
            raise write_error

    # ── WIRING ────────────────────────────────────────────────────────────────

    checks = [check.override(task_id=f"check_{f.removesuffix('.csv')}")(f) for f in config.BASE_FILES]
    # A straight line: a failure anywhere marks every later task upstream_failed,
    # so audit_run (all_done) only needs to follow the last one.
    transform_marts(transform_staging(load_raw(*checks, check_increments()))) >> audit_run()


nammamart_elt_samarth()
