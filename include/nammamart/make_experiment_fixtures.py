"""
include/nammamart/make_experiment_fixtures.py
---------------------------------------------
Part F: builds one input folder per experiment under include/data/nammamart_experiments/<exp>/
by copying the five base files and applying exactly one change. The source CSVs are never edited.
The folder is gitignored and can be rebuilt at any time.

Usage (host or container, standard library only):
  python include/nammamart/make_experiment_fixtures.py

Trigger an experiment with:
  {"data_dir": "include/data/nammamart_experiments/<exp>", "as_of_date": "2026-10-01"}
"""

import csv
import shutil
from pathlib import Path

INCLUDE = Path(__file__).resolve().parents[1]
BASE = INCLUDE / "data" / "nammamart"
OUT = INCLUDE / "data" / "nammamart_experiments"
BASE_FILES = ["orders.csv", "payments.csv", "customers.csv", "products.csv", "stores.csv"]


def read(path: Path):
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    return rows[0], rows[1:]


def write(path: Path, header, rows) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def fresh_copy(name: str, skip: tuple = ()) -> Path:
    folder = OUT / name
    shutil.rmtree(folder, ignore_errors=True)
    folder.mkdir(parents=True)
    for f in BASE_FILES:
        if f not in skip:
            shutil.copy(BASE / f, folder / f)
    return folder


def exp04_missing_products() -> str:
    fresh_copy("exp04_missing_products", skip=("products.csv",))
    return "products.csv left out"


def exp05_bad_rows() -> str:
    """Five new orders, each valid except for one deliberate defect."""
    folder = fresh_copy("exp05_bad_rows")
    header, rows = read(folder / "orders.csv")
    _, products = read(BASE / "products.csv")
    price = float(next(p for p in products if p[0] == "P0001")[5])        # P0001 passes its own checks

    def order(order_id, customer="C00001", store="S01", qty=2, status="DELIVERED", ts="2026-09-15 12:00:00"):
        amount = f"{round(qty * price, 2):.2f}"
        return dict(zip(header, [order_id, ts, customer, store, "P0001", str(qty), f"{price:.2f}", "0", amount,
                                 "UPI", status, "App", "20"]))

    extra = [
        order("ORD900001", customer=""),                    # null customer
        order("ORD900002", qty=-1),                         # negative quantity (amount consistent with it)
        order("ORD900003", store="S77"),                    # unknown store
        order("ORD900004", status="SHIPPED"),               # status outside the allowed list
        order("ORD900005", ts="2027-03-01 10:00:00"),       # 2027 timestamp
    ]
    write(folder / "orders.csv", header, rows + [[r[c] for c in header] for r in extra])
    return "5 rows appended to orders.csv: ORD900001–ORD900005"


def exp06_schema_change() -> str:
    folder = fresh_copy("exp06_schema_change")
    header, rows = read(folder / "orders.csv")
    write(folder / "orders.csv", ["total" if c == "order_amount" else c for c in header], rows)
    return "orders.csv header: order_amount renamed to total"


def exp07_volume_50() -> str:
    folder = fresh_copy("exp07_volume_50")
    header, rows = read(folder / "orders.csv")
    write(folder / "orders.csv", header, rows[:50])
    return "orders.csv cut to its first 50 rows"


def exp08_price_drift() -> str:
    folder = fresh_copy("exp08_price_drift_S03")
    header, rows = read(folder / "orders.csv")
    store, price = header.index("store_id"), header.index("unit_price")
    changed = 0
    for r in rows:
        if r[store].strip() == "S03":
            try:
                r[price] = f"{float(r[price]) * 100:.1f}"
                changed += 1
            except ValueError:
                pass
    write(folder / "orders.csv", header, rows)
    return f"unit_price x100 on {changed} S03 orders (order_amount unchanged)"


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    for build in [exp04_missing_products, exp05_bad_rows, exp06_schema_change, exp07_volume_50, exp08_price_drift]:
        print(f"{build.__name__:<24} {build()}")
