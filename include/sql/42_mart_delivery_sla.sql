-- MART: delivery SLA per store (business question 3).
-- Average delivery time and the share of DELIVERED orders delivered within 15 minutes.

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    o.store_id,
    s.store_name,
    s.zone,
    COUNT(*)                                                    AS delivered_orders,
    ROUND(AVG(o.delivery_minutes), 1)                           AS avg_delivery_minutes,
    COUNT(*) FILTER (WHERE o.within_15_min)                     AS within_15_min_orders,
    ROUND(100.0 * COUNT(*) FILTER (WHERE o.within_15_min) / COUNT(*), 1) AS pct_within_15_min
FROM stg_orders o
JOIN stg_stores s USING (store_id)
WHERE o.dq_status <> 'FAIL' AND o.order_status = 'DELIVERED'
GROUP BY ALL
ORDER BY o.store_id;
