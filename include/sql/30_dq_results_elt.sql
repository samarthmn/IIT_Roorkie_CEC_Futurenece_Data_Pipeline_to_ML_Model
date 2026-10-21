-- dq_results for the ELT: one row per check per run, the same check names as the ETL
-- (include/nammamart/quality.py), so dag_id is the only difference between the two scorecards.
-- Row-level checks count the staging rows whose failure_reason / warning_reason contains one of the
-- check's reason codes. Uniqueness counts the extra copies in stg_rejected_duplicates.
-- Freshness and volume are dataset-level checks (rows_checked = 1).

DELETE FROM dq_results
WHERE run_id = (SELECT run_id FROM run_ctx) AND dag_id = (SELECT dag_id FROM run_ctx);

INSERT INTO dq_results
WITH
reasons AS (
    SELECT 'orders' AS f, failure_reason AS fr, warning_reason AS wr FROM stg_orders
    UNION ALL SELECT 'payments', failure_reason, warning_reason FROM stg_payments
    UNION ALL SELECT 'customers', failure_reason, warning_reason FROM stg_customers
    UNION ALL SELECT 'products', failure_reason, warning_reason FROM stg_products
    UNION ALL SELECT 'stores', failure_reason, warning_reason FROM stg_stores
),
canonical_rows AS (
    SELECT f, COUNT(*) AS n FROM reasons GROUP BY f
),
catalog (check_name, f, dimension, severity, codes) AS (
    VALUES
    ('orders_required_fields',             'orders',    'Completeness', 'Critical',
        'missing_order_id|missing_customer_id|missing_store_id|missing_product_id|missing_order_ts'),
    ('orders_delivered_has_minutes',       'orders',    'Completeness', 'Critical', 'delivered_missing_delivery_minutes'),
    ('orders_status_valid',                'orders',    'Validity',     'Critical', 'invalid_order_status'),
    ('orders_status_standardised',         'orders',    'Validity',     'Warning',  'order_status_standardised'),
    ('orders_payment_mode_valid',          'orders',    'Validity',     'Critical', 'invalid_payment_mode'),
    ('orders_payment_mode_standardised',   'orders',    'Validity',     'Warning',  'payment_mode_standardised'),
    ('orders_ts_parses',                   'orders',    'Validity',     'Critical', 'invalid_order_ts'),
    ('orders_in_expected_period',          'orders',    'Validity',     'Critical', 'outside_expected_period'),
    ('orders_discount_range',              'orders',    'Validity',     'Critical', 'invalid_discount_pct'),
    ('orders_quantity_positive',           'orders',    'Accuracy',     'Critical', 'invalid_quantity|nonpositive_quantity'),
    ('orders_amount_formula',              'orders',    'Accuracy',     'Critical', 'order_amount_mismatch'),
    ('orders_delivery_max',                'orders',    'Accuracy',     'Critical', 'invalid_delivery_minutes|delivery_minutes_over_120'),
    ('orders_delivery_normal_range',       'orders',    'Accuracy',     'Warning',  'delivery_minutes_outside_8_35'),
    ('orders_fk_customer',                 'orders',    'Consistency',  'Critical', 'unknown_customer_id'),
    ('orders_fk_product',                  'orders',    'Consistency',  'Critical', 'unknown_product_id'),
    ('orders_fk_store',                    'orders',    'Consistency',  'Critical', 'unknown_store_id'),
    ('orders_not_future',                  'orders',    'Timeliness',   'Critical', 'future_order_ts'),
    ('payments_payment_mode_valid',        'payments',  'Validity',     'Critical', 'invalid_payment_mode'),
    ('payments_payment_mode_standardised', 'payments',  'Validity',     'Warning',  'payment_mode_standardised'),
    ('payments_ts_parses',                 'payments',  'Validity',     'Critical', 'invalid_payment_ts'),
    ('payments_amount_positive',           'payments',  'Accuracy',     'Critical', 'nonpositive_amount_paid'),
    ('payments_fk_order',                  'payments',  'Consistency',  'Critical', 'unknown_order_id'),
    ('payments_amount_matches_order',      'payments',  'Consistency',  'Warning',  'amount_differs_from_order'),
    ('payments_not_future',                'payments',  'Timeliness',   'Critical', 'future_payment_ts'),
    ('customers_email_format',             'customers', 'Validity',     'Warning',  'invalid_email'),
    ('customers_phone_10_digits',          'customers', 'Validity',     'Warning',  'invalid_phone'),
    ('customers_pincode_6_digits',         'customers', 'Validity',     'Warning',  'invalid_pincode'),
    ('customers_loyalty_tier_casing',      'customers', 'Consistency',  'Warning',  'loyalty_tier_standardised'),
    ('customers_signup_not_future',        'customers', 'Timeliness',   'Critical', 'future_signup_date'),
    ('products_category_present',          'products',  'Completeness', 'Critical', 'missing_category'),
    ('products_mrp_positive',              'products',  'Accuracy',     'Critical', 'nonpositive_mrp'),
    ('products_cost_le_mrp',               'products',  'Accuracy',     'Critical', 'cost_above_mrp'),
    ('stores_zone_present',                'stores',    'Completeness', 'Warning',  'missing_zone'),
    ('stores_pincode_6_digits',            'stores',    'Validity',     'Warning',  'invalid_pincode')
),
row_checks AS (
    SELECT c.check_name, c.dimension, c.severity, n.n AS rows_checked,
        (SELECT COUNT(*) FROM reasons r
         WHERE r.f = c.f
           AND regexp_matches(COALESCE(CASE WHEN c.severity = 'Critical' THEN r.fr ELSE r.wr END, ''),
                              '(^|;)(' || c.codes || ')(;|$)')) AS rows_failed
    FROM catalog c JOIN canonical_rows n ON n.f = c.f
),
raw_rows AS (
    SELECT 'orders' AS f, 'order_id' AS k, COUNT(*) AS n FROM raw_orders WHERE _run_id = (SELECT run_id FROM run_ctx)
    UNION ALL SELECT 'payments', 'payment_id', COUNT(*) FROM raw_payments WHERE _run_id = (SELECT run_id FROM run_ctx)
    UNION ALL SELECT 'customers', 'customer_id', COUNT(*) FROM raw_customers WHERE _run_id = (SELECT run_id FROM run_ctx)
    UNION ALL SELECT 'products', 'product_id', COUNT(*) FROM raw_products WHERE _run_id = (SELECT run_id FROM run_ctx)
    UNION ALL SELECT 'stores', 'store_id', COUNT(*) FROM raw_stores WHERE _run_id = (SELECT run_id FROM run_ctx)
),
uniqueness AS (
    SELECT r.f || '_' || r.k || '_unique' AS check_name, 'Uniqueness' AS dimension, 'Critical' AS severity,
           r.n AS rows_checked,
           (SELECT COUNT(*) FROM stg_rejected_duplicates d WHERE d.failure_reason = 'duplicate_' || r.k) AS rows_failed
    FROM raw_rows r
),
freshness AS (
    SELECT 'orders_freshness' AS check_name, 'Timeliness' AS dimension, 'Warning' AS severity, 1 AS rows_checked,
           CASE WHEN MAX(order_ts) IS NULL
                  OR ROUND(date_diff('second', MAX(order_ts), (SELECT as_of_ts FROM run_ctx)) / 3600.0, 1) > 24
                THEN 1 ELSE 0 END AS rows_failed
    FROM stg_orders WHERE dq_status <> 'FAIL'
),
volume AS (
    -- Warning when this run has under 10% of the previous run's orders (no baseline on the first run)
    SELECT 'orders_volume_vs_previous' AS check_name, 'Completeness' AS dimension, 'Warning' AS severity,
           1 AS rows_checked,
           CASE WHEN prev.n > 0 AND (SELECT COUNT(*) FROM stg_orders) < prev.n * 0.10 THEN 1 ELSE 0 END AS rows_failed
    FROM (SELECT MAX(rows_checked) FILTER (WHERE rn = 1) AS n
          FROM (SELECT rows_checked, ROW_NUMBER() OVER (ORDER BY checked_at DESC) AS rn
                FROM dq_results
                WHERE dag_id = (SELECT dag_id FROM run_ctx) AND check_name = 'orders_required_fields')) AS prev
),
all_checks AS (
    SELECT * FROM row_checks
    UNION ALL SELECT * FROM uniqueness
    UNION ALL SELECT * FROM freshness
    UNION ALL SELECT * FROM volume
)
SELECT
    (SELECT run_id FROM run_ctx), (SELECT dag_id FROM run_ctx),
    check_name, dimension, severity, rows_checked, rows_failed,
    CASE WHEN rows_failed = 0 THEN 'PASS' WHEN severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END,
    CAST(now() AS TIMESTAMP)
FROM all_checks;
