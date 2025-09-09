import datetime

from pyspark.sql import functions as F
from pyspark.sql.session import SparkSession

from src.utils.datetime_utils import timestamp_to_date_col, timestamp_to_time_col


def test_timestamp_to_date_col(spark: SparkSession) -> None:
    # Create a DataFrame with a known timestamp column using a datetime object
    data = [(datetime.datetime(2025, 4, 10, 10, 30, 0),)]
    schema = 'order_timestamp timestamp'
    df = spark.createDataFrame(data, schema=schema)

    # Use the utility to add a date column
    result_df = timestamp_to_date_col(df, 'order_timestamp', 'order_date')

    # Assert that the extracted date matches the expected value
    row = result_df.select('order_date').first()

    expected_date = datetime.date(2025, 4, 10)  # Expected: 2025-04-10

    assert row is not None
    assert row['order_date'] == expected_date


def test_timestamp_to_time_col(spark: SparkSession) -> None:
    # Create a DataFrame with a datetime column
    data = [(1, '2025-04-10 16:07:45')]
    columns = ['id', 'order_timestamp']

    # Convert the datetime to timestamp
    df = spark.createDataFrame(data, columns).withColumn(
        'order_timestamp',
        F.to_timestamp(F.col('order_timestamp')),
    )

    # Apply the utility to extract time
    result_df = timestamp_to_time_col(df, 'order_timestamp', 'order_time')

    # Validate the time is properly formatted as string HH:mm:ss
    row = result_df.select('order_time').first()

    assert row is not None
    assert row['order_time'] == '16:07:45'
