-- STAGING: orders, step 2 of 3 (incremental load).
-- Run once per increment file, oldest first; the file name comes from the temp table merge_ctx.
-- MERGE on order_id: an existing order (a correction, e.g. DELIVERED -> RETURNED) is overwritten,
-- a new order is inserted, so no order_id is ever duplicated. The raw rows stay in raw_orders.
-- Increment rows must not be dated after the date in the file name (decision D9).

INSERT INTO stg_rejected_duplicates
SELECT _source_file, order_id, _source_row, 'duplicate_order_id', _run_id
FROM raw_orders
WHERE _run_id = (SELECT run_id FROM run_ctx) AND _source_file = (SELECT source_file FROM merge_ctx)
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _source_row) > 1;

MERGE INTO stg_orders AS t
USING (
    SELECT
        order_id, order_ts, customer_id, store_id, product_id, quantity, unit_price, discount_pct,
        order_amount, payment_mode, order_status, channel, delivery_minutes,
        CAST(NULL AS TIMESTAMP) AS _period_start,
        CAST(regexp_extract(_source_file, 'orders_(\d{4}-\d{2}-\d{2})_increment', 1) || ' 23:59:59' AS TIMESTAMP)
                                AS _period_end,
        _source_file, _source_row, _run_id
    FROM raw_orders
    WHERE _run_id = (SELECT run_id FROM run_ctx) AND _source_file = (SELECT source_file FROM merge_ctx)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _source_row) = 1
) AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
    order_ts = s.order_ts, customer_id = s.customer_id, store_id = s.store_id, product_id = s.product_id,
    quantity = s.quantity, unit_price = s.unit_price, discount_pct = s.discount_pct,
    order_amount = s.order_amount, payment_mode = s.payment_mode, order_status = s.order_status,
    channel = s.channel, delivery_minutes = s.delivery_minutes,
    _period_start = s._period_start, _period_end = s._period_end,
    _source_file = s._source_file, _source_row = s._source_row, _run_id = s._run_id
WHEN NOT MATCHED THEN INSERT (
    order_id, order_ts, customer_id, store_id, product_id, quantity, unit_price, discount_pct,
    order_amount, payment_mode, order_status, channel, delivery_minutes,
    _period_start, _period_end, _source_file, _source_row, _run_id
) VALUES (
    s.order_id, s.order_ts, s.customer_id, s.store_id, s.product_id, s.quantity, s.unit_price, s.discount_pct,
    s.order_amount, s.payment_mode, s.order_status, s.channel, s.delivery_minutes,
    s._period_start, s._period_end, s._source_file, s._source_row, s._run_id
);
