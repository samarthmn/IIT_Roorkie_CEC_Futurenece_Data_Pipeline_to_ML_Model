-- STAGING: stores. One row per store_id (first copy in file order).
-- Warning rules: stores_zone_present (blank zone -> 'Unknown'), stores_pincode_6_digits.
-- manager_name (employee personal data) is not carried into staging.

INSERT INTO stg_rejected_duplicates
SELECT _source_file, store_id, _source_row, 'duplicate_store_id', _run_id
FROM raw_stores
WHERE _run_id = (SELECT run_id FROM run_ctx)
QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY _source_file, _source_row) > 1;

CREATE OR REPLACE TABLE stg_stores AS
WITH src AS (
    SELECT * FROM raw_stores
    WHERE _run_id = (SELECT run_id FROM run_ctx)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY _source_file, _source_row) = 1
),
checked AS (
    SELECT *,
        CAST(NULL AS VARCHAR) AS fr,
        NULLIF(concat_ws(';',
            CASE WHEN TRIM(zone) = '' THEN 'missing_zone' END,
            CASE WHEN NOT regexp_matches(TRIM(pincode), '^\d{6}$') THEN 'invalid_pincode' END
        ), '') AS wr
    FROM src
)
SELECT
    TRIM(store_id)                                              AS store_id,
    NULLIF(TRIM(store_name), '')                                AS store_name,
    COALESCE(NULLIF(TRIM(zone), ''), 'Unknown')                 AS zone,
    NULLIF(TRIM(area), '')                                      AS area,
    NULLIF(TRIM(pincode), '')                                   AS pincode,
    CAST(TRY_STRPTIME(TRIM(open_date), '%Y-%m-%d') AS DATE)     AS open_date,
    TRY_CAST(TRIM(capacity_orders_per_day) AS INTEGER)          AS capacity_orders_per_day,
    CASE WHEN fr IS NOT NULL THEN 'FAIL' WHEN wr IS NOT NULL THEN 'WARN' ELSE 'PASS' END AS dq_status,
    fr                                                          AS failure_reason,
    wr                                                          AS warning_reason,
    _source_file, _source_row, _run_id
FROM checked
ORDER BY _source_row;
