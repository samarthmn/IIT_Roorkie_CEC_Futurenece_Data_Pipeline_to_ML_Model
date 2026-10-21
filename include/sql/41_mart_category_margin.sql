-- MART: revenue and gross margin by product category (business question 2).
-- gross_margin = revenue - quantity x cost_price, DELIVERED orders only.
-- Products with a blank category are reported as 'Unknown' (their orders still count, decision D7).
-- A product that failed a cost check (nonpositive_mrp, nonpositive_cost_price, cost_above_mrp; mirrors
-- config.UNTRUSTED_COST_REASONS) has an untrustworthy cost: its revenue counts, but it is left out of
-- cost and gross_margin and shown separately as revenue_cost_untrusted.

CREATE OR REPLACE TABLE mart_category_margin AS
WITH delivered AS (
    SELECT o.net_revenue, o.quantity, p.category, p.cost_price,
           NOT regexp_matches(COALESCE(p.failure_reason, ''),
                              '(^|;)(nonpositive_mrp|nonpositive_cost_price|cost_above_mrp)(;|$)') AS cost_trusted
    FROM stg_orders o
    JOIN stg_products p USING (product_id)
    WHERE o.dq_status <> 'FAIL' AND o.order_status = 'DELIVERED'
)
SELECT
    category,
    COUNT(*)                                                                    AS delivered_orders,
    SUM(net_revenue)                                                            AS revenue,
    SUM(quantity * cost_price) FILTER (WHERE cost_trusted)                      AS cost,
    SUM(net_revenue - quantity * cost_price) FILTER (WHERE cost_trusted)        AS gross_margin,
    ROUND(100 * SUM(net_revenue - quantity * cost_price) FILTER (WHERE cost_trusted)
          / NULLIF(SUM(net_revenue) FILTER (WHERE cost_trusted), 0), 2)         AS margin_pct,
    COALESCE(SUM(net_revenue) FILTER (WHERE NOT cost_trusted), 0)               AS revenue_cost_untrusted
FROM delivered
GROUP BY category
ORDER BY revenue DESC;
