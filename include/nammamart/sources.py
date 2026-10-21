"""
include/nammamart/sources.py
-----------------------
Source-file access shared by both DAGs: data-dir resolution, header checks, strict CSV reads.

Failure behaviour (Part F):
  - missing file          -> FileNotFoundError      (Airflow retries: the file may arrive late)
  - missing/renamed column -> AirflowFailException (no retries: a schema change will not fix itself)
"""

import csv
import os
import re
from pathlib import Path

from include.nammamart import config


def resolve_data_dir(data_dir: str) -> Path:
    """Resolve the DAG param against AIRFLOW_HOME and refuse anything outside include/data."""
    root = (Path(config.INCLUDE_DIR) / "data").resolve()
    path = (Path(config.AIRFLOW_HOME) / data_dir).resolve()
    if path != root and root not in path.parents:
        raise ValueError(f"data_dir must be inside {root}, got {path}")
    return path


def check_header(path: Path, expected: list[str]) -> list[str]:
    """Raise if the file is missing (retryable) or a column is missing (not retryable).
    Returns the list of unexpected extra columns (a Warning, logged by the caller)."""
    from airflow.sdk.exceptions import AirflowFailException

    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {path}")
    with open(path, newline="", encoding="utf-8") as f:
        header = next(csv.reader(f), [])
    missing = [c for c in expected if c not in header]
    if missing:
        raise AirflowFailException(
            f"Schema check failed for {path.name}: missing column(s) {missing}; header was {header}"
        )
    return [c for c in header if c not in expected]


def read_source(path: Path, expected: list[str]):
    """Read every value as text, exactly as received; add the 1-based data row number."""
    import pandas as pd

    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[])
    df = df[expected].copy()
    df["_source_row"] = range(1, len(df) + 1)
    df["_source_file"] = path.name
    return df


def list_increments(data_dir: Path) -> list[Path]:
    """orders_<YYYY-MM-DD>_increment.csv files, oldest first."""
    return sorted(data_dir.glob(config.INCREMENT_GLOB), key=lambda p: increment_date(p.name))


def increment_date(file_name: str) -> str:
    m = re.match(r"orders_(\d{4}-\d{2}-\d{2})_increment\.csv$", file_name)
    if not m:
        raise ValueError(f"Unexpected increment file name: {file_name}")
    return m.group(1)


def count_data_rows(path: Path) -> int:
    with open(path, newline="", encoding="utf-8") as f:
        return sum(1 for _ in csv.reader(f)) - 1


def ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path
