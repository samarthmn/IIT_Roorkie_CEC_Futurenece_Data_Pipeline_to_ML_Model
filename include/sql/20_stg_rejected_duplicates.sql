-- STAGING: side table for extra copies of a duplicated key (decision D4).
-- Each stg_* table keeps one canonical row per key; the other copies are listed here so they are
-- set aside with a reason instead of silently disappearing. Rebuilt every run from this run's raw batch.

CREATE OR REPLACE TABLE stg_rejected_duplicates (
    file_name       VARCHAR,
    row_key         VARCHAR,
    _source_row     BIGINT,
    failure_reason  VARCHAR,
    _run_id         VARCHAR
);
