from pyspark.sql.functions import to_date, col

def timestamp_to_date_col(spark, df, timestamp_col, output_col):
    """
    Extracts the date from a timestamp column and adds it as a new column in the DataFrame.
    
    Parameters:
        spark: Spark Session.
        df (DataFrame): Input PySpark DataFrame containing the timestamp.
        timestamp_col (str): The name of the column containing the timestamp.
        output_col (str): The name for the output column with the date.
    
    Returns:
        DataFrame: A new DataFrame with the additional date column.
    """
    # Use to_date to extract the date part of the timestamp
    return df.withColumn(output_col, to_date(col(timestamp_col)))