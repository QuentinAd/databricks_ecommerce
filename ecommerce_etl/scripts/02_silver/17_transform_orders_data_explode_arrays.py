spark.sql("""
CREATE OR REPLACE TEMP VIEW tv_orders_exploded
AS
SELECT json_value.order_id,
        json_value.order_status,
        json_value.payment_method,
        json_value.total_amount,
        json_value.transaction_timestamp,
        json_value.customer_id,
        explode(array_distinct(json_value.items)) AS item
FROM gizmobox.silver.orders_json;
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS gizmobox.silver.orders
AS
SELECT order_id,
       order_status,
       payment_method,
       total_amount,
       transaction_timestamp,
       customer_id,
       item.item_id,
       item.name,
       item.price,
       item.quantity,
       item.category,
       item.details.brand,
       item.details.color
  FROM tv_orders_exploded;
""")
