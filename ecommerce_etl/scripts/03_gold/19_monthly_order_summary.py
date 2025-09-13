import sys

# Set parameter values
catalog = sys.argv[1]

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.customer_order_summary_monthly
AS
SELECT
customer_id,
date_format(transaction_timestamp, 'yyyy-MM') AS transaction_month,
COUNT(DISTINCT order_id) AS total_orders,
SUM(quantity) as total_items_bought,
SUM(price * quantity) AS total_amount
FROM {catalog}.silver.orders
GROUP BY customer_id, transaction_month
ORDER BY customer_id, transaction_month;
""")
