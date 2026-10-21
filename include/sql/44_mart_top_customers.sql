-- MART: customers ranked by spend (business question 5). No clear email or phone:
-- only the masked email and the loyalty tier from stg_customers.
-- spend = net revenue of the customer's DELIVERED orders. Ties broken by customer_id.

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY SUM(o.net_revenue) DESC, o.customer_id) AS spend_rank,
    o.customer_id,
    c.email_masked,
    c.loyalty_tier,
    COUNT(*)                                                    AS delivered_orders,
    SUM(o.net_revenue)                                          AS spend
FROM stg_orders o
JOIN stg_customers c USING (customer_id)
WHERE o.dq_status <> 'FAIL' AND o.order_status = 'DELIVERED'
GROUP BY o.customer_id, c.email_masked, c.loyalty_tier
ORDER BY spend_rank;
