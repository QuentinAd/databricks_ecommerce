import sys

# Set parameter values
catalog = sys.argv[1]

spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW tv_orders_fixed AS
SELECT value,
       regexp_replace(value,
          '"order_date": (\\d{4}-\\d{2}-\\d{2})',
          '"order_date": "\\$1"') AS fixed_value
  FROM {catalog}.bronze.v_orders;
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.orders_json
AS
SELECT from_json(fixed_value,
          'STRUCT<customer_id: BIGINT,
          items: ARRAY<STRUCT<
          category: STRING,
          details: STRUCT<brand: STRING,
          color: STRING>,
          item_id: BIGINT, name: STRING,
          price: BIGINT,
          quantity: BIGINT>>,
          order_date: STRING, order_id: BIGINT,
          order_status: STRING, payment_method: STRING,
          total_amount: BIGINT, transaction_timestamp: STRING>') AS json_value,
       fixed_value
  FROM tv_orders_fixed;

""")
