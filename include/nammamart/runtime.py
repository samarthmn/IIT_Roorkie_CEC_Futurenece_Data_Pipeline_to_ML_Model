"""
include/nammamart/runtime.py
----------------------------
Run identity and reference times shared by both DAGs.

  run_ts : the run's wall-clock start in IST (naive). Used by the "not in the future" checks.
           Taken from dag_run.start_date so retries and Clears of the same run use the same value.
  as_of  : reference time for the freshness check = DAG param as_of_date at 09:00 IST,
           else the run's logical date (or run_after) in IST.
"""

import re

import pendulum

from include.nammamart import config


def run_info(context) -> dict:
    dag_run = context["dag_run"]
    run_id = dag_run.run_id
    tz = pendulum.timezone(config.TIMEZONE)

    start = dag_run.start_date or pendulum.now("UTC")
    run_ts = pendulum.instance(start).in_timezone(tz).naive()

    as_of_param = context["params"].get("as_of_date")
    if as_of_param:
        as_of = pendulum.parse(as_of_param, tz=tz).replace(hour=9, minute=0, second=0).naive()
    else:
        ref = context.get("logical_date") or dag_run.run_after or start
        as_of = pendulum.instance(ref).in_timezone(tz).naive()

    return {
        "run_id": run_id,
        "safe_run_id": re.sub(r"[^A-Za-z0-9_.-]", "_", run_id),
        "dag_id": dag_run.dag_id,
        "run_ts": run_ts.strftime(config.TS_FORMAT),
        "as_of": as_of.strftime(config.TS_FORMAT),
        "run_date_ist": run_ts.strftime(config.DATE_FORMAT),
    }
