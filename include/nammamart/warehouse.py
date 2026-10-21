"""
include/nammamart/warehouse.py
------------------------------
DuckDB access shared by both DAGs. Every task that calls connect() must run in the
"duckdb_samarth" pool: DuckDB allows one read-write process per file.

All writes here are idempotent per (run_id, dag_id), so retries and Clears never duplicate rows.
"""

import os
from contextlib import contextmanager

from include.nammamart import config


@contextmanager
def connect(read_only: bool = False):
    import duckdb

    con = duckdb.connect(config.WAREHOUSE_DB, read_only=read_only)
    con.execute(f"SET TimeZone = '{config.TIMEZONE}'")     # now()::TIMESTAMP is IST, like run_ts
    try:
        yield con
    finally:
        con.close()


def run_sql_file(con, name: str) -> None:
    """Execute a whole .sql file (DuckDB runs multi-statement scripts; no splitting on ';')."""
    with open(os.path.join(config.SQL_DIR, name), encoding="utf-8") as f:
        con.execute(f.read())


def ensure_control_tables(con) -> None:
    run_sql_file(con, "00_create_control_tables.sql")


def set_run_ctx(con, run: dict) -> None:
    """SQL files read run parameters from this temp table (parameters can't be bound across a script)."""
    con.execute(
        "CREATE OR REPLACE TEMP TABLE run_ctx AS "
        "SELECT ?::VARCHAR AS run_id, ?::VARCHAR AS dag_id, ?::TIMESTAMP AS run_ts, ?::TIMESTAMP AS as_of_ts",
        [run["run_id"], run["dag_id"], run["run_ts"], run["as_of"]],
    )


def previous_rows_checked(con, dag_id: str, run_id: str, check_name: str):
    """rows_checked of the most recent earlier run of this DAG for one check (volume baseline)."""
    row = con.execute(
        """
        SELECT rows_checked FROM dq_results
        WHERE dag_id = ? AND run_id <> ? AND check_name = ?
        ORDER BY checked_at DESC LIMIT 1
        """,
        [dag_id, run_id, check_name],
    ).fetchone()
    return row[0] if row else None


def write_dq_results(con, run: dict, results: list[dict]) -> None:
    import pandas as pd

    df = pd.DataFrame(results)
    df["run_id"] = run["run_id"]
    df["dag_id"] = run["dag_id"]
    con.register("dq_df", df)
    con.begin()
    try:
        con.execute("DELETE FROM dq_results WHERE run_id = ? AND dag_id = ?", [run["run_id"], run["dag_id"]])
        con.execute(
            """
            INSERT INTO dq_results
            SELECT run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, now()
            FROM dq_df
            """
        )
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.unregister("dq_df")


def write_dq_check(con, run: dict, result: dict) -> None:
    """Upsert one check's row for this run (used by the ELT audit, whose other checks are written in SQL)."""
    con.begin()
    try:
        con.execute("DELETE FROM dq_results WHERE run_id = ? AND dag_id = ? AND check_name = ?",
                    [run["run_id"], run["dag_id"], result["check_name"]])
        con.execute("INSERT INTO dq_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, CAST(now() AS TIMESTAMP))",
                    [run["run_id"], run["dag_id"], result["check_name"], result["dimension"], result["severity"],
                     result["rows_checked"], result["rows_failed"], result["status"]])
        con.commit()
    except Exception:
        con.rollback()
        raise


def upsert_pipeline_run(con, row: dict) -> None:
    con.begin()
    try:
        con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [row["run_id"], row["dag_id"]])
        con.execute(
            "INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [row["run_id"], row["dag_id"], row["start_time"], row["end_time"], row["rows_extracted"],
             row["rows_loaded"], row["rows_quarantined"], row["status"]],
        )
        con.commit()
    except Exception:
        con.rollback()
        raise
