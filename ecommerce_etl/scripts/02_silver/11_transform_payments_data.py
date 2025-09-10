import sys

# Set parameter values
catalog = sys.argv[1]

spark.sql(f"""
CREATE TABLE {catalog}.silver.payments
AS
SELECT payment_id,
       order_id,
       date_format(payment_timestamp, 'yyyy-MM-dd') AS payment_date,
       date_format(payment_timestamp, 'HH:mm:ss') AS payment_time,
       CASE payment_status
         WHEN 1 THEN 'Success'
         WHEN 2 THEN 'Pending'
         WHEN 3 THEN 'Cancelled'
         WHEN 4 THEN 'Failed'
       END AS payment_status,
       payment_method
  FROM {catalog}.bronze.payments;
  """)
