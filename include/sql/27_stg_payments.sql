-- STAGING: payments. One row per payment_id (first copy in file order).
-- Mirrors quality.validate_payments: same reason codes, in the same order.
-- unknown_order_id: the order_id is in none of this run's orders files.
-- amount_differs_from_order (Warning): a SUCCESS amount differs from the merged order's order_amount;
-- every such case also appears in mart_payment_reconciliation.

INSERT INTO stg_rejected_duplicates
SELECT _source_file, payment_id, _source_row, 'duplicate_payment_id', _run_id
FROM raw_payments
WHERE _run_id = (SELECT run_id FROM run_ctx)
QUALIFY ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY _source_file, _source_row) > 1;

CREATE OR REPLACE TABLE stg_payments AS
WITH
source_order_ids AS (
    SELECT DISTINCT TRIM(order_id) AS k FROM raw_orders
    WHERE _run_id = (SELECT run_id FROM run_ctx) AND TRIM(order_id) <> ''
),
src AS (
    SELECT p.*,
        UPPER(TRIM(p.payment_status))                           AS status_std,
        CASE LOWER(TRIM(p.payment_mode))
            WHEN 'upi' THEN 'UPI'
            WHEN 'card' THEN 'Card'
            WHEN 'cod' THEN 'Cash on Delivery'
            WHEN 'cash on delivery' THEN 'Cash on Delivery'
            WHEN 'wallet' THEN 'Wallet'
        END                                                     AS mode_std,
        TRY_STRPTIME(TRIM(p.payment_ts), '%Y-%m-%d %H:%M:%S')   AS ts,
        TRY_CAST(TRIM(p.amount_paid) AS DOUBLE)                 AS paid,
        TRY_CAST(o.order_amount_raw AS DOUBLE)                  AS order_amount
    FROM raw_payments p
    LEFT JOIN stg_orders o ON o.order_id = TRIM(p.order_id)
    WHERE p._run_id = (SELECT run_id FROM run_ctx)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.payment_id ORDER BY p._source_file, p._source_row) = 1
),
reasons AS (
    SELECT *,
        NULLIF(concat_ws(';',
            CASE WHEN mode_std IS NULL THEN 'invalid_payment_mode' END,
            CASE WHEN ts IS NULL THEN 'invalid_payment_ts' END,
            CASE WHEN status_std = 'SUCCESS' AND (paid IS NULL OR paid <= 0) THEN 'nonpositive_amount_paid' END,
            CASE WHEN TRIM(order_id) NOT IN (SELECT k FROM source_order_ids) THEN 'unknown_order_id' END,
            CASE WHEN ts > (SELECT run_ts FROM run_ctx) THEN 'future_payment_ts' END
        ), '') AS fr,
        NULLIF(concat_ws(';',
            CASE WHEN mode_std IS NOT NULL AND mode_std <> payment_mode THEN 'payment_mode_standardised' END,
            CASE WHEN status_std = 'SUCCESS' AND order_amount IS NOT NULL AND ABS(paid - order_amount) > 0.010000001
                 THEN 'amount_differs_from_order' END
        ), '') AS wr
    FROM src
)
SELECT
    TRIM(payment_id)                                AS payment_id,
    TRIM(order_id)                                  AS order_id,
    ts                                              AS payment_ts,
    mode_std                                        AS payment_mode,
    CAST(paid AS DECIMAL(12,2))                     AS amount_paid,
    status_std                                      AS payment_status,
    NULLIF(TRIM(gateway_ref), '')                   AS gateway_ref,
    CASE WHEN fr IS NOT NULL THEN 'FAIL' WHEN wr IS NOT NULL THEN 'WARN' ELSE 'PASS' END AS dq_status,
    fr                                              AS failure_reason,
    wr                                              AS warning_reason,
    payment_ts      AS payment_ts_raw,
    payment_mode    AS payment_mode_raw,
    amount_paid     AS amount_paid_raw,
    _source_file, _source_row, _run_id
FROM reasons
ORDER BY _source_row;
