spark.sql("""
CREATE TABLE IF NOT EXISTS gizmobox.gold.customer_order_summary_monthly
AS
SELECT
customer_id,
date_format(transaction_timestamp, 'yyyy-MM') AS transaction_month,
COUNT(DISTINCT order_id) AS total_orders,
SUM(quantity) as total_items_bought,
SUM(price * quantity) AS total_amount
FROM gizmobox.silver.orders
GROUP BY customer_id, transaction_month
ORDER BY customer_id, transaction_month;
""")
