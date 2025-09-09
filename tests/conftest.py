import os
import sys

from pyspark.sql import SparkSession
import pytest

# Run the tests from the root directory
sys.path.append(os.getcwd())


# Returning a Spark Session
@pytest.fixture
def spark() -> 'SparkSession':
    try:
        from databricks.connect import DatabricksSession

        spark = DatabricksSession.builder.getOrCreate()
    except ImportError:
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
        except Exception as err:
            raise ImportError(
                'Neither Databricks Session or Spark Session are available',
            ) from err
    return spark
