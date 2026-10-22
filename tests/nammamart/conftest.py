"""
Shared fixtures for the NammaMart tests: a tiny, fictional dataset that contains one example of every
defect type found in the real files (Part 0), so each rule and both pipelines can be tested quickly.
"""

import csv
import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from include.nammamart import config  # noqa: E402

RUN = {
    "run_id": "manual__test", "safe_run_id": "manual__test", "dag_id": "test_dag",
    "run_ts": "2026-10-21 09:00:00", "as_of": "2026-10-01 09:00:00", "run_date_ist": "2026-10-21",
}

V = "2026-09-10 10:00:00"          # a valid September timestamp
ORDERS = [
    # order_id, order_ts, customer_id, store_id, product_id, quantity, unit_price, discount_pct, order_amount,
    # payment_mode, order_status, channel, delivery_minutes
    ["ORD01", V, "C1", "S01", "P0001", "2", "100.0", "0", "200.0", "UPI", "DELIVERED", "App", "20"],      # valid
    ["ORD01", V, "C1", "S01", "P0001", "2", "100.0", "0", "200.0", "UPI", "DELIVERED", "App", "20"],      # exact duplicate
    ["ORD03", V, "", "S01", "P0001", "1", "100.0", "0", "100.0", "UPI", "DELIVERED", "App", "20"],        # blank customer
    ["ORD04", V, "C1", "S01", "P0001", "-1", "100.0", "0", "-100.0", "UPI", "DELIVERED", "App", "20"],    # negative quantity
    ["ORD05", V, "C1", "S99", "P0001", "1", "100.0", "0", "100.0", "UPI", "DELIVERED", "App", "20"],      # unknown store
    ["ORD06", V, "C1", "S01", "P0001", "1", "100.0", "0", "100.0", "UPI", "Pending??", "App", "20"],      # bad status
    ["ORD07", V, "C1", "S01", "P0001", "2", "100.0", "0", "200.0", "UPI", "delivered", "App", "20"],      # status case (warn)
    ["ORD08", V, "C1", "S01", "P0001", "2", "100.0", "0", "200.0", "upi ", "DELIVERED", "App", "20"],     # mode alias (warn)
    ["ORD09", V, "C1", "S01", "P0001", "2", "100.0", "0", "200.0", "Cashh", "DELIVERED", "App", "20"],    # ambiguous mode
    ["ORD10", "N/A", "C1", "S01", "P0001", "1", "100.0", "0", "100.0", "UPI", "DELIVERED", "App", "20"],  # unparsable ts
    ["ORD11", "2026-10-15 10:00:00", "C1", "S01", "P0001", "1", "100.0", "0", "100.0", "UPI", "DELIVERED", "App", "20"],
    ["ORD12", "2027-01-01 09:00:00", "C1", "S01", "P0001", "1", "100.0", "0", "100.0", "UPI", "DELIVERED", "App", "20"],
    ["ORD13", V, "C1", "S01", "P0001", "1", "100.0", "150", "-50.0", "UPI", "DELIVERED", "App", "20"],    # discount 150
    ["ORD14", V, "C1", "S01", "P0001", "2", "100.0", "0", "2000.0", "UPI", "DELIVERED", "App", "20"],     # amount x10
    ["ORD15", V, "C1", "S01", "P0001", "1", "100.0", "0", "100.0", "UPI", "DELIVERED", "App", ""],        # no minutes
    ["ORD16", V, "C1", "S01", "P0001", "1", "100.0", "0", "100.0", "UPI", "DELIVERED", "App", "480"],     # hours
    ["ORD17", V, "C1", "S01", "P0001", "1", "100.0", "0", "100.0", "UPI", "DELIVERED", "App", "-5"],      # negative minutes
    ["ORD18", V, "C1", "S01", "P0001", "2", "-100.0", "0", "-200.0", "UPI", "DELIVERED", "App", "20"],    # negative price
    ["ORD19", V, "C2", "S02", "P0002", "1", "100.0", "0", "100.0", "UPI", "DELIVERED", "App", "12"],      # product cost failed
    ["ORD20", V, "C3", "S01", "P0003", "1", "100.0", "0", "100.0", "UPI", "CANCELLED", "App", ""],        # cancelled, no minutes
    ["ORD21", V, "C1", "S01", "P0001", "1", "100.0", "10", "90.0", "Card", "DELIVERED", "Web", "40"],     # slow (warn), unpaid
    ["ORD22", V, "C1", "S01", "P9999", "1", "100.0", "0", "100.0", "UPI", "DELIVERED", "App", "20"],      # unknown product
]
INCREMENT = [
    ["ORD01", V, "C1", "S01", "P0001", "2", "100.0", "0", "200.0", "UPI", "RETURNED", "App", "20"],       # correction
    ["ORD30", "2026-10-01 11:00:00", "C1", "S01", "P0001", "1", "100.0", "0", "100.0", "UPI", "DELIVERED", "App", "15"],
    ["ORD31", "2026-10-05 11:00:00", "C1", "S01", "P0001", "1", "100.0", "0", "100.0", "UPI", "DELIVERED", "App", "15"],
]
PAYMENTS = [
    ["PAY1", "ORD01", "2026-09-10 10:05:00", "UPI", "200.0", "SUCCESS", "GW1"],
    ["PAY1", "ORD01", "2026-09-10 10:05:00", "UPI", "200.0", "SUCCESS", "GW1"],   # duplicate
    ["PAY2", "ORD999", "2026-09-10 10:05:00", "UPI", "50.0", "SUCCESS", "GW2"],   # unknown order
    ["PAY3", "ORD07", "2026-09-10 10:05:00", "UPI", "150.0", "SUCCESS", "GW3"],   # amount mismatch
    ["PAY4", "ORD09", "2026-09-10 10:05:00", "Cashh", "200.0", "SUCCESS", ""],    # rejected order, bad mode
    ["PAY5", "ORD08", "2026-09-10 10:05:00", "UPI", "200.0", "FAILED", "GW5"],    # only payment failed
    ["PAY6", "ORD19", "2026-09-10 10:05:00", "Card", "100.0", "SUCCESS", "GW6"],
]
CUSTOMERS = [
    ["C1", "Test One", "test.one@example.com", "9000000001", "Bengaluru", "Area A", "560001", "2025-01-01", "Gold", "Y"],
    ["C2", "Test Two", "N/A", "9000000002", "Bengaluru", "Area B", "560002", "2025-01-01", "Silver", "Y"],
    ["C2", "Test Two", "test.two@example.com", "9000000002", "Bengaluru", "Area B", "560002", "2025-01-01", "Silver", "Y"],
    ["C3", "Test Three", "test.three@example", "90000003", "", "Area C", "5600XX", "2027-01-01", "bronze", "N"],
]
PRODUCTS = [
    ["P0001", "Rice 1kg", "Staples", "Rice", "BrandA", "100.0", "60.0", "kg", "False", "2024-01-01"],
    ["P0001", "Rice 1kg v2", "Staples", "Rice", "BrandA", "90.0", "60.0", "kg", "False", "2024-01-01"],   # duplicate
    ["P0002", "Soap", "Personal Care", "Soap", "BrandB", "100.0", "120.0", "unit", "False", "2024-01-01"],  # cost > mrp
    ["P0003", "Milk", "", "Milk", "BrandC", "50.0", "30.0", "pack", "True", "2024-01-01"],                # no category
    ["P0004", "Oil", "Staples", "Oil", "BrandD", "-10.0", "-5.0", "unit", "False", "2024-01-01"],          # negative
]
STORES = [
    ["S01", "Store One", "North", "Area A", "560001", "2024-01-01", "Manager One", "100"],
    ["S02", "Store Two", "", "Area B", "560002", "2024-01-01", "Manager Two", "100"],                     # no zone
]
FILES = {"orders.csv": ORDERS, "payments.csv": PAYMENTS, "customers.csv": CUSTOMERS,
         "products.csv": PRODUCTS, "stores.csv": STORES, "orders_2026-10-01_increment.csv": INCREMENT}


def frame(file_name: str, rows: list) -> pd.DataFrame:
    """The DataFrame read_source() would return for these rows."""
    cols = config.EXPECTED_COLUMNS.get(file_name, config.ORDER_COLUMNS)
    df = pd.DataFrame(rows, columns=cols, dtype=str)
    df["_source_row"] = range(1, len(df) + 1)
    df["_source_file"] = file_name
    return df


@pytest.fixture
def run():
    return dict(RUN)


@pytest.fixture
def data_dir(tmp_path) -> Path:
    """The fixture files written as CSVs, as they would arrive in include/data/nammamart/."""
    for name, rows in FILES.items():
        cols = config.EXPECTED_COLUMNS.get(name, config.ORDER_COLUMNS)
        with open(tmp_path / name, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(cols)
            w.writerows(rows)
    return tmp_path
