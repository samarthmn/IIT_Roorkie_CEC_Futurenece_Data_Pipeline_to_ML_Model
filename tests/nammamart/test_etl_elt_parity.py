"""
ETL ↔ ELT parity (Part B.4): the same fixture files go through the pandas rules (ETL) and the real
include/sql files (ELT) in a throwaway DuckDB file. Every check count, every order's reasons, the masked
customers, revenue, margin and the reconciliation must be identical. If a rule is changed on one side only,
this test fails.
"""

import duckdb
import pandas as pd
import pytest

from include.nammamart import config, elt, etl, quality, warehouse
from include.nammamart.sources import list_increments, read_source

STAGING = ["00_create_control_tables.sql", "10_raw_create.sql", "20_stg_rejected_duplicates.sql", "21_stg_stores.sql",
           "22_stg_products.sql", "23_stg_customers.sql", "24_stg_orders_build.sql"]
MARTS = ["40_mart_daily_store_kpis.sql", "41_mart_category_margin.sql", "42_mart_delivery_sla.sql",
         "43_mart_payment_reconciliation.sql", "44_mart_top_customers.sql"]


def _etl(data_dir, run):
    raw = {f: read_source(data_dir / f, config.EXPECTED_COLUMNS[f]) for f in config.BASE_FILES}
    incs = [read_source(p, config.ORDER_COLUMNS) for p in list_increments(data_dir)]
    stores, _, r1 = quality.validate_stores(raw["stores.csv"])
    products, _, r2 = quality.validate_products(raw["products.csv"])
    customers, _, r3 = quality.validate_customers(raw["customers.csv"], run)
    keys = {n: set(raw[f][k].str.strip()) - {""} for n, f, k in
            [("customers", "customers.csv", "customer_id"), ("products", "products.csv", "product_id"),
             ("stores", "stores.csv", "store_id")]}
    orders, _, r4, _, _ = quality.validate_orders(raw["orders.csv"], incs, keys, run)
    ids = set(pd.concat([raw["orders.csv"]["order_id"], *[i["order_id"] for i in incs]]).str.strip()) - {""}
    payments, pay_q, r5 = quality.validate_payments(raw["payments.csv"], ids, orders, run)
    results = r1 + r2 + r3 + r4 + r5 + [quality.volume_check(len(orders), None)]
    dim_p = etl.build_dim_product(products)
    fact = etl.build_fact(orders[orders["dq_status"] != "FAIL"], dim_p, run)
    recon = etl.reconcile(fact, orders, payments, pay_q[pay_q["failure_reason"] == "duplicate_payment_id"], ids)
    return {"results": results, "orders": orders, "customers": etl.build_dim_customer(customers),
            "dim_product": dim_p, "fact": fact, "recon": recon}


def _elt(data_dir, run, db_path):
    con = duckdb.connect(str(db_path))
    con.execute(f"SET TimeZone = '{config.TIMEZONE}'")
    for name in STAGING[:2]:
        warehouse.run_sql_file(con, name)
    files = config.BASE_FILES + [p.name for p in list_increments(data_dir)]
    for f in files:
        rows, inserted = elt.load_raw_file(con, str(data_dir / f), f, run)
        assert inserted and rows > 0
    warehouse.set_run_ctx(con, run)
    for name in STAGING[2:]:
        warehouse.run_sql_file(con, name)
    for f in sorted(f for f in files if f.startswith("orders_")):
        con.execute("CREATE OR REPLACE TEMP TABLE merge_ctx AS SELECT ? AS source_file", [f])
        warehouse.run_sql_file(con, "25_stg_orders_merge_increment.sql")
    for name in ["26_stg_orders_apply_dq.sql", "27_stg_payments.sql", "30_dq_results_elt.sql", *MARTS]:
        warehouse.run_sql_file(con, name)
    return con


@pytest.fixture
def both(data_dir, run, tmp_path):
    con = _elt(data_dir, run, tmp_path / "parity.duckdb")
    yield _etl(data_dir, run), con
    con.close()


def test_every_check_has_identical_counts(both):
    etl_side, con = both
    e = {(r["check_name"], r["rows_checked"], r["rows_failed"], r["status"]) for r in etl_side["results"]}
    l = set(con.execute("SELECT check_name, rows_checked, rows_failed, status FROM dq_results").fetchall())
    assert e == l
    assert len(e) == 43            # + source_files_schema, written by audit_run in both DAGs = 44 per run


def test_every_order_has_identical_status_and_reasons(both):
    etl_side, con = both
    norm = lambda v: v if isinstance(v, str) else None
    e = {(r.order_id, r.dq_status, norm(r.failure_reason), norm(r.warning_reason))
         for r in etl_side["orders"].itertuples()}
    l = set(con.execute("SELECT order_id, dq_status, failure_reason, warning_reason FROM stg_orders").fetchall())
    assert e == l


def test_masked_customers_match(both):
    etl_side, con = both
    cols = ["customer_id", "email_masked", "phone_masked", "loyalty_tier", "dq_status"]
    e = set(etl_side["customers"][cols].astype(object).where(etl_side["customers"][cols].notna(), None)
            .itertuples(index=False, name=None))
    l = set(con.execute(f"SELECT {', '.join(cols)} FROM stg_customers").fetchall())
    assert e == l


def test_failed_master_values_are_not_published(both):
    etl_side, con = both
    e = etl_side["dim_product"].set_index("product_id")[["mrp", "cost_price"]]
    l = con.execute("SELECT product_id, mrp, cost_price FROM stg_products").df().set_index("product_id")
    for pid in ["P0001", "P0002", "P0003", "P0004"]:
        for col in ["mrp", "cost_price"]:
            ev, lv = e.loc[pid, col], l.loc[pid, col]
            assert (pd.isna(ev) and pd.isna(lv)) or float(ev) == float(lv), (pid, col)


def test_revenue_and_trusted_margin_match(both):
    etl_side, con = both
    fact = etl_side["fact"]
    delivered = fact[fact["order_status"] == "DELIVERED"]
    revenue, margin, untrusted = con.execute(
        "SELECT SUM(revenue), SUM(gross_margin), SUM(revenue_cost_untrusted) FROM mart_category_margin").fetchone()
    assert float(revenue) == pytest.approx(delivered["net_revenue"].sum())
    assert float(margin) == pytest.approx(float(delivered["gross_margin"].sum()))
    assert float(untrusted) == pytest.approx(delivered.loc[delivered["gross_margin"].isna(), "net_revenue"].sum())
    assert float(con.execute("SELECT SUM(net_revenue) FROM mart_daily_store_kpis").fetchone()[0]) == \
        pytest.approx(delivered["net_revenue"].sum())


def test_reconciliation_matches(both):
    etl_side, con = both
    pid = lambda v: v if isinstance(v, str) else None                 # no payment: NaN in pandas, NULL in SQL
    e = {(r.order_id, pid(r.payment_id), r.reason, round(float(r.difference), 2)) for r in etl_side["recon"].itertuples()}
    l = {(o, p, r, round(float(d), 2)) for o, p, r, d in
         con.execute("SELECT order_id, payment_id, reason, difference FROM mart_payment_reconciliation").fetchall()}
    assert e == l


def test_raw_keeps_rows_exactly_and_is_never_reloaded(both, data_dir, run):
    _, con = both
    assert con.execute("SELECT COUNT(*) FROM raw_orders WHERE customer_id = ''").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM raw_orders WHERE order_ts = 'N/A'").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM raw_orders WHERE payment_mode = 'upi '").fetchone()[0] == 1
    before = con.execute("SELECT COUNT(*), MIN(_loaded_at) FROM raw_orders").fetchone()
    rows, inserted = elt.load_raw_file(con, str(data_dir / "orders.csv"), "orders.csv", run)   # a retry / Clear
    assert not inserted
    assert con.execute("SELECT COUNT(*), MIN(_loaded_at) FROM raw_orders").fetchone() == before


def test_merge_applies_the_increment_without_duplicates(both):
    _, con = both
    assert con.execute("SELECT COUNT(*) = COUNT(DISTINCT order_id) FROM stg_orders").fetchone()[0]
    assert con.execute("SELECT order_status FROM stg_orders WHERE order_id = 'ORD01'").fetchone()[0] == "RETURNED"


def test_no_clear_personal_data_in_staging_or_marts(both):
    _, con = both
    leaks = con.execute("""SELECT table_name, column_name FROM information_schema.columns
                           WHERE (table_name LIKE 'stg_%' OR table_name LIKE 'mart_%')
                             AND column_name IN ('email', 'phone', 'full_name', 'gateway_ref', 'manager_name')""").fetchall()
    assert leaks == []
