spark.sql("""DROP TABLE IF EXISTS gizmobox.bronze.payments;
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS gizmobox.bronze.payments
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
