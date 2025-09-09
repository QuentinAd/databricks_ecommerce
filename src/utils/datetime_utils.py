from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


def timestamp_to_date_col(
    df: DataFrame,
    timestamp_col: str,
    output_col: str,
) -> DataFrame:
    """
    Extracts the date from a timestamp column and adds it as a new column.

    Parameters:
        spark: Spark Session.
        df (DataFrame): Input PySpark DataFrame containing the timestamp.
        timestamp_col (str): The name of the column containing the timestamp.
        output_col (str): The name for the output column with the date.

    Returns:
        DataFrame: A new DataFrame with the additional date column.
    """
    # Extract the date part of the timestamp as a true DateType column
    # Use to_date on the timestamp column to avoid returning a string
    return df.withColumn(output_col, F.to_date(F.col(timestamp_col)))


def timestamp_to_time_col(
    df: DataFrame,
    timestamp_col: str,
    output_col: str,
) -> DataFrame:
    """
    Extracts the time (HH:mm:ss) from a timestamp column and adds it as a new column.

    Parameters:
        spark: Spark Session.
        df (DataFrame): Input PySpark DataFrame containing the timestamp.
        timestamp_col (str): The name of the column containing the timestamp.
        output_col (str): The name for the output column with the time string.

    Returns:
        DataFrame: A new DataFrame with the additional time column.
    """
    return df.withColumn(output_col, F.date_format(F.col(timestamp_col), 'HH:mm:ss'))
