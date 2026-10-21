-- MART: one row per order/payment mismatch, one primary reason each (business question 4,
-- acceptance criterion 7). Same taxonomy and arithmetic as the ETL's etl.reconcile():
--   difference = SUCCESS amount paid - amount owed (order_amount if DELIVERED, else 0)
-- Every payment row is reconciled (decision D14): canonical payments, payments for rejected or unknown
-- orders, and the extra copies of duplicated payment_ids.

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH
valid_orders AS (                -- orders that reach the marts
    SELECT order_id, order_date, order_status, order_amount FROM stg_orders WHERE dq_status <> 'FAIL'
),
source_order_ids AS (            -- every order_id in this run's orders files
    SELECT DISTINCT TRIM(order_id) AS order_id FROM raw_orders
    WHERE _run_id = (SELECT run_id FROM run_ctx) AND TRIM(order_id) <> ''
),
payments AS (
    SELECT payment_id, order_id, payment_status, payment_ts, amount_paid,
           CASE WHEN payment_status = 'SUCCESS' THEN COALESCE(amount_paid, 0) ELSE 0 END AS success_paid
    FROM stg_payments
),
duplicate_copies AS (
    SELECT TRIM(r.payment_id) AS payment_id, TRIM(r.order_id) AS order_id,
           TRY_CAST(TRIM(r.amount_paid) AS DECIMAL(12,2)) AS amount_paid,
           CASE WHEN UPPER(TRIM(r.payment_status)) = 'SUCCESS'
                THEN COALESCE(TRY_CAST(TRIM(r.amount_paid) AS DECIMAL(12,2)), 0) ELSE 0 END AS success_paid
    FROM stg_rejected_duplicates d
    JOIN raw_payments r ON r._run_id = d._run_id AND r._source_file = d.file_name AND r._source_row = d._source_row
    WHERE d.failure_reason = 'duplicate_payment_id'
),
payment_level AS (
    SELECT p.order_id, p.payment_id, o.order_status, o.order_amount, p.amount_paid,
           p.success_paid AS difference,
           CASE WHEN p.order_id IN (SELECT order_id FROM source_order_ids) THEN 'ORDER_REJECTED'
                ELSE 'ORDER_NOT_FOUND' END AS reason
    FROM payments p
    LEFT JOIN stg_orders o ON o.order_id = p.order_id
    WHERE p.order_id NOT IN (SELECT order_id FROM valid_orders)
    UNION ALL
    SELECT d.order_id, d.payment_id, o.order_status, o.order_amount, d.amount_paid,
           d.success_paid, 'DUPLICATE_PAYMENT_ID'
    FROM duplicate_copies d
    LEFT JOIN stg_orders o ON o.order_id = d.order_id
),
paid_for_valid AS (
    SELECT * FROM payments WHERE order_id IN (SELECT order_id FROM valid_orders)
),
feed AS (                        -- last day the payment feed covers, for orders that reach the marts
    SELECT MAX(CAST(payment_ts AS DATE)) AS feed_end FROM paid_for_valid
),
per_order AS (
    SELECT order_id,
           string_agg(payment_id, ';' ORDER BY payment_id)      AS payment_id,
           COUNT(*)                                             AS n_payments,
           SUM(success_paid)                                    AS paid,
           bool_or(payment_status = 'SUCCESS')                  AS has_success,
           bool_or(payment_status = 'REFUNDED')                 AS has_refund
    FROM paid_for_valid
    GROUP BY order_id
),
order_level AS (
    SELECT v.order_id, a.payment_id, v.order_status, v.order_amount,
           COALESCE(a.paid, 0) AS amount_paid,
           COALESCE(a.paid, 0) - CASE WHEN v.order_status = 'DELIVERED' THEN v.order_amount ELSE 0 END AS difference,
           CASE
               WHEN v.order_status = 'DELIVERED' AND a.order_id IS NULL
                    AND v.order_date > (SELECT feed_end FROM feed)              THEN 'PAYMENT_NOT_YET_RECEIVED'
               WHEN v.order_status = 'DELIVERED' AND a.order_id IS NULL         THEN 'MISSING_PAYMENT'
               WHEN v.order_status = 'DELIVERED' AND a.has_success
                    AND ABS(a.paid - v.order_amount) > 0.010000001             THEN 'AMOUNT_MISMATCH'
               WHEN v.order_status = 'DELIVERED' AND NOT a.has_success          THEN 'PAYMENT_FAILED'
               WHEN v.order_status = 'RETURNED' AND a.has_success AND NOT a.has_refund THEN 'RETURNED_NOT_REFUNDED'
               WHEN v.order_status = 'CANCELLED' AND a.has_success              THEN 'CANCELLED_WITH_PAYMENT'
           END AS reason
    FROM valid_orders v
    LEFT JOIN per_order a USING (order_id)
)
SELECT order_id, payment_id, order_status,
       CAST(order_amount AS DECIMAL(12,2))          AS order_amount,
       CAST(amount_paid AS DECIMAL(12,2))           AS amount_paid,
       CAST(ROUND(difference, 2) AS DECIMAL(12,2))  AS difference,
       reason,
       (SELECT run_id FROM run_ctx)                 AS run_id
FROM (
    SELECT * FROM payment_level
    UNION ALL
    SELECT * FROM order_level WHERE reason IS NOT NULL
)
ORDER BY reason, order_id, payment_id;
