"""
include/nammamart/elt.py
------------------------
ELT helpers (Part B). Python only moves files into DuckDB and runs the .sql files in include/sql;
every transformation is SQL.
"""

from include.nammamart import config
from include.nammamart.sources import count_data_rows

RAW_TABLES = {
    "orders.csv": "raw_orders",
    "payments.csv": "raw_payments",
    "customers.csv": "raw_customers",
    "products.csv": "raw_products",
    "stores.csv": "raw_stores",
}
NEVER_NULL = "\x01"     # read_csv nullstr: a token that never occurs, so '' stays '' instead of NULL


def raw_table(file_name: str) -> str:
    return RAW_TABLES.get(file_name, "raw_orders")          # increment files go to raw_orders


def load_raw_file(con, path: str, file_name: str, run: dict) -> tuple[int, bool]:
    """Copy one CSV into its raw table exactly as received (all VARCHAR), in one transaction.
    Raw is never deleted: if this run already loaded this file (a retry or Clear of the same run),
    the existing batch is kept and nothing is inserted. The load is one transaction, so a batch
    is either complete or absent. Returns (rows in the batch, whether it was inserted now)."""
    expected = config.EXPECTED_COLUMNS.get(file_name, config.ORDER_COLUMNS)
    table = raw_table(file_name)
    existing = con.execute(f"SELECT COUNT(*) FROM {table} WHERE _run_id = ? AND _source_file = ?",
                           [run["run_id"], file_name]).fetchone()[0]
    if existing:
        return existing, False
    columns = "{" + ", ".join(f"'{c}': 'VARCHAR'" for c in expected) + "}"
    names = ", ".join(expected)

    con.begin()
    try:
        # Explicit column list + auto_detect=false: values land in the declared columns in file order.
        # The header itself is checked in Python before this runs (a renamed column fails loudly there).
        con.execute(
            f"""CREATE OR REPLACE TEMP TABLE raw_batch AS
                SELECT * FROM read_csv(?, header = true, auto_detect = false, columns = {columns},
                                       nullstr = '{NEVER_NULL}', quote = '"', escape = '"', strict_mode = true)""",
            [path],
        )
        con.execute(
            f"""INSERT INTO {table}
                SELECT {names},
                       rowid - (SELECT MIN(rowid) FROM raw_batch) + 1 AS _source_row,   -- 1 = first data line
                       CAST(now() AS TIMESTAMP) AS _loaded_at,
                       ? AS _source_file, ? AS _run_id
                FROM raw_batch ORDER BY rowid""",
            [file_name, run["run_id"]],
        )
        loaded = con.execute(f"SELECT COUNT(*) FROM {table} WHERE _run_id = ? AND _source_file = ?",
                             [run["run_id"], file_name]).fetchone()[0]
        expected_rows = count_data_rows(path)
        if loaded != expected_rows:
            raise ValueError(f"{file_name}: loaded {loaded} rows into {table}, file has {expected_rows}")
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.execute("DROP TABLE IF EXISTS raw_batch")
    return loaded, True


def merge_counts(con, file_name: str) -> dict:
    """Before the MERGE: how many increment orders are new (insert) vs existing (update)."""
    row = con.execute(
        """SELECT COUNT(*) FILTER (WHERE order_id NOT IN (SELECT order_id FROM stg_orders)),
                  COUNT(*) FILTER (WHERE order_id IN (SELECT order_id FROM stg_orders))
           FROM (SELECT DISTINCT order_id FROM raw_orders
                 WHERE _run_id = (SELECT run_id FROM run_ctx) AND _source_file = ?)""",
        [file_name],
    ).fetchone()
    return {"file": file_name, "inserted": row[0], "updated": row[1]}
