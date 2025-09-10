import sys

# Set parameter values
catalog = sys.argv[1]

spark.sql(f"""DROP TABLE IF EXISTS {catalog}.bronze.payments;
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.payments
  (payment_id INTEGER,
  order_id INTEGER,
  payment_timestamp TIMESTAMP,
  payment_status INTEGER,
  payment_method STRING)
USING CSV
OPTIONS (
  header = "true",
  delimiter = ","
)
LOCATION 's3://databricks-dea-demo/gizmobox/landing/external_data/payments';
          """)
