-- STAGING: orders, step 1 of 3 (build).
-- Canonical rows from this run's orders.csv batch: one row per order_id (first copy in file order),
-- still as raw text. 25_stg_orders_merge_increment.sql then upserts any increment files and
-- 26_stg_orders_apply_dq.sql types, standardises and validates the merged rows.
-- _period_start/_period_end implement decision D9: orders.csv must be inside 1–30 Sept 2026
-- (mirrors config.EXPECTED_PERIOD).

INSERT INTO stg_rejected_duplicates
SELECT _source_file, order_id, _source_row, 'duplicate_order_id', _run_id
FROM raw_orders
WHERE _run_id = (SELECT run_id FROM run_ctx) AND _source_file = 'orders.csv'
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _source_row) > 1;

CREATE OR REPLACE TABLE stg_orders AS
SELECT
    order_id, order_ts, customer_id, store_id, product_id, quantity, unit_price, discount_pct,
    order_amount, payment_mode, order_status, channel, delivery_minutes,
    TIMESTAMP '2026-09-01 00:00:00' AS _period_start,
    TIMESTAMP '2026-09-30 23:59:59' AS _period_end,
    _source_file, _source_row, _run_id
FROM raw_orders
WHERE _run_id = (SELECT run_id FROM run_ctx) AND _source_file = 'orders.csv'
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _source_row) = 1;
