import sys

from pyspark.sql import functions as F

from utils.datetime_utils import timestamp_to_date_col, timestamp_to_time_col

# Set parameter values
catalog = sys.argv[1]

# Read bronze refunds
df = spark.table('gizmobox.bronze.refunds')

# Derive refund_date using shared datetime utility
df = timestamp_to_date_col(df, 'refund_timestamp', 'refund_date')

# Derive refund_time using shared datetime utility and parse refund_reason/source
df = timestamp_to_time_col(df, 'refund_timestamp', 'refund_time')
df = df.withColumn(
    'refund_reason',
    F.regexp_extract(F.col('refund_reason'), r'^([^:]+):', 1),
).withColumn('refund_source', F.regexp_extract(F.col('refund_reason'), r'^[^:]+:(.*)$', 1))

# Select final schema
refunds_silver_df = df.select(
    'refund_id',
    'payment_id',
    'refund_date',
    'refund_time',
    'refund_amount',
    'refund_reason',
    'refund_source',
)

# Write to silver table (idempotent overwrite)
refunds_silver_df.write.mode('overwrite').saveAsTable(f'{catalog}.silver.refunds')
