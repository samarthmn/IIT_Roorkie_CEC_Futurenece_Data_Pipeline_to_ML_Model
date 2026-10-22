-- STAGING: products. One row per product_id (first copy in file order: orders are priced at the
-- first copy's mrp for P0030 and P0140).
-- Critical rules: products_category_present, products_mrp_positive, products_cost_positive, products_cost_le_mrp.
-- A product that fails stays in stg_products with dq_status = 'FAIL' so its orders still join (decision D7),
-- but the value that failed is published as NULL (a negative mrp, an untrusted cost); raw keeps the original.

INSERT INTO stg_rejected_duplicates
SELECT _source_file, product_id, _source_row, 'duplicate_product_id', _run_id
FROM raw_products
WHERE _run_id = (SELECT run_id FROM run_ctx)
QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _source_file, _source_row) > 1;

CREATE OR REPLACE TABLE stg_products AS
WITH src AS (
    SELECT *,
        TRY_CAST(TRIM(mrp) AS DOUBLE)        AS mrp_num,
        TRY_CAST(TRIM(cost_price) AS DOUBLE) AS cost_num
    FROM raw_products
    WHERE _run_id = (SELECT run_id FROM run_ctx)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _source_file, _source_row) = 1
),
checked AS (
    SELECT *,
        NULLIF(concat_ws(';',
            CASE WHEN TRIM(category) = '' THEN 'missing_category' END,
            CASE WHEN mrp_num IS NULL OR mrp_num <= 0 THEN 'nonpositive_mrp' END,
            CASE WHEN cost_num IS NULL OR cost_num <= 0 THEN 'nonpositive_cost_price' END,
            CASE WHEN cost_num > mrp_num THEN 'cost_above_mrp' END
        ), '') AS fr,
        CAST(NULL AS VARCHAR) AS wr
    FROM src
)
SELECT
    TRIM(product_id)                                            AS product_id,
    NULLIF(TRIM(product_name), '')                              AS product_name,
    COALESCE(NULLIF(TRIM(category), ''), 'Unknown')             AS category,
    NULLIF(TRIM(sub_category), '')                              AS sub_category,
    NULLIF(TRIM(brand), '')                                     AS brand,
    CASE WHEN NOT regexp_matches(COALESCE(fr, ''), '(^|;)nonpositive_mrp(;|$)')
         THEN CAST(mrp_num AS DECIMAL(12,2)) END                AS mrp,
    CASE WHEN NOT regexp_matches(COALESCE(fr, ''), '(^|;)(nonpositive_mrp|nonpositive_cost_price|cost_above_mrp)(;|$)')
         THEN CAST(cost_num AS DECIMAL(12,2)) END               AS cost_price,
    NULLIF(TRIM(unit), '')                                      AS unit,
    LOWER(TRIM(is_perishable)) = 'true'                         AS is_perishable,
    CAST(TRY_STRPTIME(TRIM(launch_date), '%Y-%m-%d') AS DATE)   AS launch_date,
    CASE WHEN fr IS NOT NULL THEN 'FAIL' WHEN wr IS NOT NULL THEN 'WARN' ELSE 'PASS' END AS dq_status,
    fr                                                          AS failure_reason,
    wr                                                          AS warning_reason,
    _source_file, _source_row, _run_id
FROM checked
ORDER BY _source_row;
