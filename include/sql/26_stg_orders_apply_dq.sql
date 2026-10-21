-- STAGING: orders, step 3 of 3 (type, standardise, validate).
-- Runs on the merged rows so base and increment rows get exactly the same rules.
-- Mirrors quality.validate_orders: same reason codes, in the same order.
-- Foreign keys are checked against the keys present in this run's raw master files (decision D7):
-- a master row that failed its own checks does not fail the order.
-- The original text of every parsed column is kept as <column>_raw.

CREATE OR REPLACE TABLE stg_orders AS
WITH
customer_keys AS (
    SELECT DISTINCT TRIM(customer_id) AS k FROM raw_customers
    WHERE _run_id = (SELECT run_id FROM run_ctx) AND TRIM(customer_id) <> ''
),
product_keys AS (
    SELECT DISTINCT TRIM(product_id) AS k FROM raw_products
    WHERE _run_id = (SELECT run_id FROM run_ctx) AND TRIM(product_id) <> ''
),
store_keys AS (
    SELECT DISTINCT TRIM(store_id) AS k FROM raw_stores
    WHERE _run_id = (SELECT run_id FROM run_ctx) AND TRIM(store_id) <> ''
),
parsed AS (
    SELECT *,
        UPPER(TRIM(order_status)) AS status_std,
        CASE LOWER(TRIM(payment_mode))
            WHEN 'upi' THEN 'UPI'
            WHEN 'card' THEN 'Card'
            WHEN 'cod' THEN 'Cash on Delivery'
            WHEN 'cash on delivery' THEN 'Cash on Delivery'
            WHEN 'wallet' THEN 'Wallet'
        END                                                     AS mode_std,      -- 'Cashh' stays NULL (D6)
        TRY_STRPTIME(TRIM(order_ts), '%Y-%m-%d %H:%M:%S')       AS ts,
        TRY_CAST(TRIM(quantity) AS DOUBLE)                      AS q,
        TRY_CAST(TRIM(unit_price) AS DOUBLE)                    AS price,
        TRY_CAST(TRIM(discount_pct) AS DOUBLE)                  AS disc,
        TRY_CAST(TRIM(order_amount) AS DOUBLE)                  AS amount,
        TRY_CAST(TRIM(delivery_minutes) AS DOUBLE)              AS minutes
    FROM stg_orders
),
checked AS (
    SELECT *,
        ROUND(q * price * (1 - disc / 100), 2) AS expected_amount
    FROM parsed
),
reasons AS (
    SELECT *,
        NULLIF(concat_ws(';',
            -- Completeness
            CASE WHEN TRIM(order_id) = '' THEN 'missing_order_id' END,
            CASE WHEN TRIM(customer_id) = '' THEN 'missing_customer_id' END,
            CASE WHEN TRIM(store_id) = '' THEN 'missing_store_id' END,
            CASE WHEN TRIM(product_id) = '' THEN 'missing_product_id' END,
            CASE WHEN TRIM(order_ts) = '' THEN 'missing_order_ts' END,
            CASE WHEN status_std = 'DELIVERED' AND TRIM(delivery_minutes) = '' THEN 'delivered_missing_delivery_minutes' END,
            -- Validity
            CASE WHEN status_std NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'invalid_order_status' END,
            CASE WHEN mode_std IS NULL THEN 'invalid_payment_mode' END,
            CASE WHEN TRIM(order_ts) <> '' AND ts IS NULL THEN 'invalid_order_ts' END,
            CASE WHEN ts IS NOT NULL AND ((_period_start IS NOT NULL AND ts < _period_start) OR ts > _period_end)
                 THEN 'outside_expected_period' END,
            CASE WHEN disc IS NULL OR disc < 0 OR disc > 100 THEN 'invalid_discount_pct' END,
            -- Accuracy
            CASE WHEN q IS NULL OR q <> ROUND(q) THEN 'invalid_quantity' END,
            CASE WHEN q <= 0 THEN 'nonpositive_quantity' END,
            CASE WHEN price IS NULL OR price <= 0 THEN 'nonpositive_unit_price' END,
            CASE WHEN amount IS NULL OR expected_amount IS NULL OR ABS(expected_amount - amount) > 0.010000001
                 THEN 'order_amount_mismatch' END,
            CASE WHEN TRIM(delivery_minutes) <> '' AND (minutes IS NULL OR minutes <> ROUND(minutes))
                 THEN 'invalid_delivery_minutes' END,
            CASE WHEN minutes < 0 THEN 'negative_delivery_minutes' END,
            CASE WHEN minutes > 120 THEN 'delivery_minutes_over_120' END,
            -- Consistency
            CASE WHEN TRIM(customer_id) <> '' AND TRIM(customer_id) NOT IN (SELECT k FROM customer_keys)
                 THEN 'unknown_customer_id' END,
            CASE WHEN TRIM(product_id) <> '' AND TRIM(product_id) NOT IN (SELECT k FROM product_keys)
                 THEN 'unknown_product_id' END,
            CASE WHEN TRIM(store_id) <> '' AND TRIM(store_id) NOT IN (SELECT k FROM store_keys)
                 THEN 'unknown_store_id' END,
            -- Timeliness
            CASE WHEN ts > (SELECT run_ts FROM run_ctx) THEN 'future_order_ts' END
        ), '') AS fr,
        NULLIF(concat_ws(';',
            CASE WHEN status_std IN ('DELIVERED', 'CANCELLED', 'RETURNED') AND status_std <> order_status
                 THEN 'order_status_standardised' END,
            CASE WHEN mode_std IS NOT NULL AND mode_std <> payment_mode THEN 'payment_mode_standardised' END,
            CASE WHEN minutes >= 0 AND minutes <= 120 AND (minutes < 8 OR minutes > 35)
                 THEN 'delivery_minutes_outside_8_35' END
        ), '') AS wr
    FROM checked
)
SELECT
    TRIM(order_id)                                  AS order_id,
    ts                                              AS order_ts,
    CAST(ts AS DATE)                                AS order_date,
    TRIM(customer_id)                               AS customer_id,
    TRIM(store_id)                                  AS store_id,
    TRIM(product_id)                                AS product_id,
    TRY_CAST(q AS INTEGER)                          AS quantity,
    CAST(price AS DECIMAL(12,2))                    AS unit_price,
    TRY_CAST(disc AS DECIMAL(6,2))                  AS discount_pct,
    CAST(ROUND(amount, 2) AS DECIMAL(12,2))         AS order_amount,
    mode_std                                        AS payment_mode,
    status_std                                      AS order_status,
    NULLIF(TRIM(channel), '')                       AS channel,
    TRY_CAST(minutes AS INTEGER)                    AS delivery_minutes,
    CAST(CASE WHEN status_std = 'DELIVERED' THEN ROUND(amount, 2) ELSE 0 END AS DECIMAL(12,2)) AS net_revenue,
    COALESCE(status_std = 'DELIVERED' AND minutes <= 15, FALSE)                              AS within_15_min,
    CASE WHEN fr IS NOT NULL THEN 'FAIL' WHEN wr IS NOT NULL THEN 'WARN' ELSE 'PASS' END     AS dq_status,
    fr                                              AS failure_reason,
    wr                                              AS warning_reason,
    order_ts         AS order_ts_raw,
    quantity         AS quantity_raw,
    unit_price       AS unit_price_raw,
    discount_pct     AS discount_pct_raw,
    order_amount     AS order_amount_raw,
    payment_mode     AS payment_mode_raw,
    order_status     AS order_status_raw,
    delivery_minutes AS delivery_minutes_raw,
    _period_start, _period_end, _source_file, _source_row, _run_id
FROM reasons
ORDER BY _source_file, _source_row;
