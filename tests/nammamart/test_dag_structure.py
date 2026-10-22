"""
Structure tests for both NammaMart DAGs (Parts A, B, C and F): schedule, timezone, retries, parallel extracts,
the DuckDB pool on every warehouse task, and an audit task that always runs after everything else.
"""

from datetime import timedelta
from pathlib import Path

import pytest
from airflow.models import DagBag

from include.nammamart import config

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"
SPEC = {
    "nammamart_etl_samarth": {
        "parallel": ["extract_orders", "extract_payments", "extract_customers", "extract_products", "extract_stores",
                     "extract_increments"],
        "warehouse": ["load_star_schema", "reconcile_payments", "build_reports", "audit_run"],
    },
    "nammamart_elt_samarth": {
        "parallel": ["check_orders", "check_payments", "check_customers", "check_products", "check_stores",
                     "check_increments"],
        "warehouse": ["load_raw", "transform_staging", "transform_marts", "audit_run"],
    },
}


@pytest.fixture(scope="module")
def dagbag():
    bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert not {k: v for k, v in bag.import_errors.items() if "nammamart" in k}
    return bag


@pytest.mark.parametrize("dag_id", SPEC)
def test_schedule_is_9am_ist_monday_to_saturday(dagbag, dag_id):
    dag = dagbag.dags[dag_id]
    assert dag.schedule == "0 9 * * 1-6"
    assert type(dag.timetable).__name__ == "CronTriggerTimetable"      # Airflow 3: runs at 09:00 itself
    assert str(dag.timezone) == "Asia/Kolkata"          # from the IST start_date; Airflow stores start_date as UTC
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.dagrun_timeout == timedelta(minutes=25)


@pytest.mark.parametrize("dag_id", SPEC)
def test_retries_and_timeouts(dagbag, dag_id):
    args = dagbag.dags[dag_id].default_args
    assert args["retries"] >= 2
    assert args["retry_delay"] == timedelta(minutes=2)
    assert args["execution_timeout"] == timedelta(minutes=20)


@pytest.mark.parametrize("dag_id", SPEC)
def test_extract_tasks_run_in_parallel(dagbag, dag_id):
    dag = dagbag.dags[dag_id]
    for task_id in SPEC[dag_id]["parallel"]:
        assert dag.get_task(task_id).upstream_task_ids == set(), task_id


@pytest.mark.parametrize("dag_id", SPEC)
def test_every_warehouse_task_uses_the_duckdb_pool(dagbag, dag_id):
    dag = dagbag.dags[dag_id]
    for task_id in SPEC[dag_id]["warehouse"]:
        assert dag.get_task(task_id).pool == config.POOL, task_id
    for task_id in SPEC[dag_id]["parallel"]:
        assert dag.get_task(task_id).pool != config.POOL, task_id      # file checks stay parallel


@pytest.mark.parametrize("dag_id", SPEC)
def test_audit_runs_last_and_always(dagbag, dag_id):
    dag = dagbag.dags[dag_id]
    audit = dag.get_task("audit_run")
    assert getattr(audit.trigger_rule, "value", audit.trigger_rule) == "all_done"
    assert audit.downstream_task_ids == set()
    upstream = audit.get_flat_relative_ids(upstream=True)
    assert upstream == set(dag.task_ids) - {"audit_run"}               # every task finishes before the audit
