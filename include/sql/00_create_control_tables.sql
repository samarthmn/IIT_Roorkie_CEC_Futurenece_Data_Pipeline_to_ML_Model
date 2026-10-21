-- Control tables shared by nammamart_etl_samarth and nammamart_elt_samarth.
-- Both keep history across runs: one row per check per run, one row per run.

CREATE TABLE IF NOT EXISTS dq_results (
    run_id        VARCHAR,
    dag_id        VARCHAR,
    check_name    VARCHAR,
    dimension     VARCHAR,
    severity      VARCHAR,     -- Critical | Warning
    rows_checked  BIGINT,
    rows_failed   BIGINT,
    status        VARCHAR,     -- PASS (0 failed) | WARN (Warning check failed) | FAIL (Critical check failed: rows quarantined)
    checked_at    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id            VARCHAR,
    dag_id            VARCHAR,
    start_time        TIMESTAMP,
    end_time          TIMESTAMP,
    rows_extracted    BIGINT,
    rows_loaded       BIGINT,
    rows_quarantined  BIGINT,
    status            VARCHAR  -- SUCCESS | SUCCESS_WITH_WARNINGS | FAILED
);
