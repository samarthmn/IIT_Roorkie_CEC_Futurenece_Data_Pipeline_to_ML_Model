"""
Unit tests for the NammaMart helpers: masking, source-file checks, every data-quality rule,
the increment merge, the dimensions and fact (including the trusted-cost margin), and reconciliation.
"""

import pandas as pd
import pytest
from airflow.sdk.exceptions import AirflowFailException

from include.nammamart import config, etl, quality
from include.nammamart.masking import mask_email, mask_phone
from include.nammamart.sources import check_header, count_data_rows, list_increments, read_source, resolve_data_dir

from conftest import CUSTOMERS, INCREMENT, ORDERS, PAYMENTS, PRODUCTS, STORES, frame


# ── Masking (acceptance criterion 8) ──────────────────────────────────────────

@pytest.mark.parametrize("raw, masked", [
    ("jane.doe@example.com", "ja***@example.com"),
    ("  Jane.Doe@Example.com ", "ja***@example.com"),
    ("N/A", None), ("jane.doeexample.com", None), ("jane@example", None), ("", None), (None, None),
])
def test_mask_email(raw, masked):
    assert mask_email(raw) == masked


@pytest.mark.parametrize("raw, masked", [
    ("9876543210", "98******10"), ("98765 43210", "98******10"), ("90000003", "90****03"), ("123", None), (None, None),
])
def test_mask_phone(raw, masked):
    assert mask_phone(raw) == masked


# ── Source files (Experiments 4 and 6) ────────────────────────────────────────

def test_missing_file_is_retryable(tmp_path):
    with pytest.raises(FileNotFoundError):
        check_header(tmp_path / "products.csv", config.EXPECTED_COLUMNS["products.csv"])


def test_renamed_column_fails_without_retry(data_dir):
    path = data_dir / "orders.csv"
    text = path.read_text().replace("order_amount", "total", 1)
    path.write_text(text)
    with pytest.raises(AirflowFailException, match="order_amount"):
        check_header(path, config.ORDER_COLUMNS)


def test_read_source_keeps_values_exactly(data_dir):
    df = read_source(data_dir / "orders.csv", config.ORDER_COLUMNS)
    assert len(df) == count_data_rows(data_dir / "orders.csv") == len(ORDERS)
    assert df.loc[df["order_id"] == "ORD03", "customer_id"].item() == ""        # blank stays blank, not NaN
    assert df.loc[df["order_id"] == "ORD10", "order_ts"].item() == "N/A"        # 'N/A' stays text
    assert df.loc[df["order_id"] == "ORD08", "payment_mode"].item() == "upi "   # trailing space kept
    assert df["_source_row"].tolist()[:2] == [1, 2]


def test_increments_listed_oldest_first(tmp_path):
    for name in ["orders_2026-10-03_increment.csv", "orders_2026-10-01_increment.csv", "orders.csv"]:
        (tmp_path / name).write_text("x\n")
    assert [p.name for p in list_increments(tmp_path)] == ["orders_2026-10-01_increment.csv",
                                                           "orders_2026-10-03_increment.csv"]


def test_data_dir_must_stay_under_include_data():
    with pytest.raises(ValueError):
        resolve_data_dir("include/../dags")


# ── Data-quality rules (Part D) ───────────────────────────────────────────────

def _orders(run, with_increment=False):
    keys = {"customers": {"C1", "C2", "C3"}, "products": {"P0001", "P0002", "P0003", "P0004"}, "stores": {"S01", "S02"}}
    incs = [frame("orders_2026-10-01_increment.csv", INCREMENT)] if with_increment else []
    return quality.validate_orders(frame("orders.csv", ORDERS), incs, keys, run)


def _reasons(checked, order_id, column="failure_reason"):
    value = checked.loc[checked["order_id"] == order_id, column].item()
    return set(value.split(";")) if isinstance(value, str) else set()


@pytest.mark.parametrize("order_id, expected", [
    ("ORD03", {"missing_customer_id"}),
    ("ORD04", {"nonpositive_quantity"}),
    ("ORD05", {"unknown_store_id"}),
    ("ORD06", {"invalid_order_status"}),
    ("ORD09", {"invalid_payment_mode"}),
    ("ORD10", {"invalid_order_ts"}),
    ("ORD11", {"outside_expected_period"}),
    ("ORD12", {"outside_expected_period", "future_order_ts"}),
    ("ORD13", {"invalid_discount_pct"}),
    ("ORD14", {"order_amount_mismatch"}),
    ("ORD15", {"delivered_missing_delivery_minutes"}),
    ("ORD16", {"delivery_minutes_over_120"}),
    ("ORD17", {"negative_delivery_minutes"}),
    ("ORD18", {"nonpositive_unit_price"}),
    ("ORD22", {"unknown_product_id"}),
])
def test_each_defect_fails_with_exactly_its_reason(run, order_id, expected):
    checked, *_ = _orders(run)
    assert _reasons(checked, order_id) == expected
    assert checked.loc[checked["order_id"] == order_id, "dq_status"].item() == "FAIL"


@pytest.mark.parametrize("order_id, warning", [
    ("ORD07", "order_status_standardised"),
    ("ORD08", "payment_mode_standardised"),
    ("ORD21", "delivery_minutes_outside_8_35"),
])
def test_fixable_values_are_standardised_and_load_with_a_warning(run, order_id, warning):
    checked, *_ = _orders(run)
    assert _reasons(checked, order_id, "warning_reason") == {warning}
    assert checked.loc[checked["order_id"] == order_id, "dq_status"].item() == "WARN"


def test_dirty_master_row_does_not_fail_its_order(run):
    checked, *_ = _orders(run)
    for order_id in ["ORD19", "ORD20"]:                     # product cost failed / product category blank
        assert checked.loc[checked["order_id"] == order_id, "dq_status"].item() == "PASS"


def test_duplicate_keeps_first_copy_and_quarantines_the_extra(run):
    checked, quarantined, results, *_ = _orders(run)
    assert (checked["order_id"] == "ORD01").sum() == 1
    extras = quarantined[quarantined["failure_reason"] == "duplicate_order_id"]
    assert extras["_source_row"].tolist() == [2]
    unique = next(r for r in results if r["check_name"] == "orders_order_id_unique")
    assert (unique["rows_checked"], unique["rows_failed"], unique["status"]) == (len(ORDERS), 1, "FAIL")


def test_increment_inserts_updates_and_checks_its_own_period(run):
    checked, _, _, merge_stats, _ = _orders(run, with_increment=True)
    assert merge_stats == [{"file": "orders_2026-10-01_increment.csv", "rows": 3, "inserted": 2, "updated": 1,
                            "duplicates": 0}]
    assert checked["order_id"].is_unique
    assert checked.loc[checked["order_id"] == "ORD01", "order_status"].item() == "RETURNED"
    assert _reasons(checked, "ORD30") == set()                          # 1 Oct is inside the file's period
    assert _reasons(checked, "ORD31") == {"outside_expected_period"}    # 5 Oct is after the file date


def test_freshness_uses_the_reference_time(run):
    *_, fresh = _orders(run)
    assert fresh["latest_order_ts"] == "2026-09-10 10:00:00" and fresh["stale"]
    *_, fresh = _orders({**run, "as_of": "2026-09-11 09:00:00"})
    assert not fresh["stale"] and fresh["lag_hours"] == 23.0


def test_volume_check():
    assert quality.volume_check(50, 2400)["status"] == "WARN"
    assert quality.volume_check(2400, 2400)["status"] == "PASS"
    assert quality.volume_check(50, None)["status"] == "PASS"          # first run: no baseline


def test_source_files_check():
    assert quality.source_files_check(5, 0)["status"] == "PASS"
    assert quality.source_files_check(5, 1) == {"check_name": "source_files_schema", "dimension": "Validity",
                                                "severity": "Critical", "rows_checked": 5, "rows_failed": 1,
                                                "status": "FAIL"}


def test_products_rules():
    checked, quarantined, _ = quality.validate_products(frame("products.csv", PRODUCTS))
    by_id = checked.set_index("product_id")
    assert by_id.loc["P0001", "mrp"] == "100.0"                         # first copy kept
    assert set(by_id.loc["P0002", "failure_reason"].split(";")) == {"cost_above_mrp"}
    assert set(by_id.loc["P0003", "failure_reason"].split(";")) == {"missing_category"}
    assert set(by_id.loc["P0004", "failure_reason"].split(";")) == {"nonpositive_mrp", "nonpositive_cost_price",
                                                                     "cost_above_mrp"}
    assert len(quarantined) == 4                                         # 3 failed + 1 extra copy


def test_customers_rules(run):
    checked, _, _ = quality.validate_customers(frame("customers.csv", CUSTOMERS), run)
    by_id = checked.set_index("customer_id")
    assert by_id.loc["C2", "email"] == "test.two@example.com"           # the valid duplicate copy is kept
    assert by_id.loc["C3", "failure_reason"] == "future_signup_date"
    assert set(by_id.loc["C3", "warning_reason"].split(";")) == {"invalid_email", "invalid_phone", "invalid_pincode",
                                                                  "loyalty_tier_standardised"}


def test_payments_rules(run):
    orders, *_ = _orders(run)
    ids = set(frame("orders.csv", ORDERS)["order_id"])
    checked, quarantined, _ = quality.validate_payments(frame("payments.csv", PAYMENTS), ids, orders, run)
    by_id = checked.set_index("payment_id")
    assert by_id.loc["PAY2", "failure_reason"] == "unknown_order_id"
    assert by_id.loc["PAY3", "warning_reason"] == "amount_differs_from_order"
    assert by_id.loc["PAY4", "failure_reason"] == "invalid_payment_mode"
    assert (quarantined["failure_reason"] == "duplicate_payment_id").sum() == 1


# ── Dimensions, fact and margin (decisions D3, D7, D13) ───────────────────────

def test_dimensions_publish_no_failed_value_and_no_clear_pii(run):
    products, _, _ = quality.validate_products(frame("products.csv", PRODUCTS))
    dim_p = etl.build_dim_product(products).set_index("product_id")
    assert pd.isna(dim_p.loc["P0002", "cost_price"]) and dim_p.loc["P0002", "mrp"] == 100.0
    assert pd.isna(dim_p.loc["P0004", "mrp"]) and pd.isna(dim_p.loc["P0004", "cost_price"])
    assert dim_p.loc["P0003", "category"] == "Unknown" and dim_p.loc["P0001", "cost_price"] == 60.0

    customers, _, _ = quality.validate_customers(frame("customers.csv", CUSTOMERS), run)
    dim_c = etl.build_dim_customer(customers).set_index("customer_id")
    assert pd.isna(dim_c.loc["C3", "signup_date"])                       # a future date is not published
    assert {"email", "phone", "full_name"}.isdisjoint(dim_c.columns)
    assert dim_c.loc["C1", "email_masked"] == "te***@example.com" and dim_c.loc["C1", "phone_masked"] == "90******01"


def test_fact_margin_refuses_an_untrusted_cost(run):
    orders, *_ = _orders(run)
    products, _, _ = quality.validate_products(frame("products.csv", PRODUCTS))
    fact = etl.build_fact(orders[orders["dq_status"] != "FAIL"], etl.build_dim_product(products), run)
    row = fact.set_index("order_id")
    assert row.loc["ORD01", "net_revenue"] == 200.0 and row.loc["ORD01", "gross_margin"] == 80.0
    assert row.loc["ORD19", "net_revenue"] == 100.0 and pd.isna(row.loc["ORD19", "gross_margin"])
    assert row.loc["ORD19", "product_dq_status"] == "FAIL"
    assert row.loc["ORD20", "net_revenue"] == 0.0                        # CANCELLED: no revenue
    assert not row.loc["ORD21", "within_15_min"] and row.loc["ORD19", "within_15_min"]
    assert fact["order_id"].is_unique


def test_reconciliation_reasons(run):
    orders, *_ = _orders(run, with_increment=True)
    products, _, _ = quality.validate_products(frame("products.csv", PRODUCTS))
    fact = etl.build_fact(orders[orders["dq_status"] != "FAIL"], etl.build_dim_product(products), run)
    ids = set(pd.concat([frame("orders.csv", ORDERS), frame("orders_2026-10-01_increment.csv", INCREMENT)])["order_id"])
    pay_checked, pay_quarantined, _ = quality.validate_payments(frame("payments.csv", PAYMENTS), ids, orders, run)
    recon = etl.reconcile(fact, orders, pay_checked,
                          pay_quarantined[pay_quarantined["failure_reason"] == "duplicate_payment_id"], ids)
    got = {(r.order_id, r.reason) for r in recon.itertuples()}
    assert got == {
        ("ORD999", "ORDER_NOT_FOUND"), ("ORD09", "ORDER_REJECTED"), ("ORD01", "DUPLICATE_PAYMENT_ID"),
        ("ORD07", "AMOUNT_MISMATCH"), ("ORD08", "PAYMENT_FAILED"), ("ORD21", "MISSING_PAYMENT"),
        ("ORD01", "RETURNED_NOT_REFUNDED"), ("ORD30", "PAYMENT_NOT_YET_RECEIVED"),
    }
    assert recon.set_index("reason").loc["AMOUNT_MISMATCH", "difference"] == -50.0
