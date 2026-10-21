-- MART: revenue and gross margin by product category (business question 2).
-- gross_margin = revenue - quantity x cost_price, DELIVERED orders only.
-- Products with a blank category are reported as 'Unknown' (their orders still count, decision D7).

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    p.category,
    COUNT(*)                                                    AS delivered_orders,
    SUM(o.net_revenue)                                          AS revenue,
    SUM(o.quantity * p.cost_price)                              AS cost,
    SUM(o.net_revenue - o.quantity * p.cost_price)              AS gross_margin,
    ROUND(100 * SUM(o.net_revenue - o.quantity * p.cost_price) / NULLIF(SUM(o.net_revenue), 0), 2) AS margin_pct
FROM stg_orders o
JOIN stg_products p USING (product_id)
WHERE o.dq_status <> 'FAIL' AND o.order_status = 'DELIVERED'
GROUP BY ALL
ORDER BY revenue DESC;
