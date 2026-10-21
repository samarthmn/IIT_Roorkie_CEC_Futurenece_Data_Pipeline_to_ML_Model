-- MART: daily KPIs per store (business question 1, plus delivery KPIs per day).
-- Grain: order_date x store. Only orders that passed every Critical check (dq_status PASS or WARN).
-- net_revenue counts DELIVERED orders only, after discount (order_amount already includes it).

CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    o.order_date,
    s.zone,
    o.store_id,
    s.store_name,
    COUNT(*)                                                    AS orders,
    COUNT(*) FILTER (WHERE o.order_status = 'DELIVERED')        AS delivered_orders,
    SUM(o.net_revenue)                                          AS net_revenue,
    ROUND(AVG(o.delivery_minutes) FILTER (WHERE o.order_status = 'DELIVERED'), 1) AS avg_delivery_minutes,
    ROUND(100.0 * COUNT(*) FILTER (WHERE o.within_15_min)
          / NULLIF(COUNT(*) FILTER (WHERE o.order_status = 'DELIVERED'), 0), 1)  AS pct_within_15_min
FROM stg_orders o
JOIN stg_stores s USING (store_id)
WHERE o.dq_status <> 'FAIL'
GROUP BY ALL
ORDER BY o.order_date, s.zone, o.store_id;
