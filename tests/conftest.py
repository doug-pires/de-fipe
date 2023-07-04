import pytest
from pyspark.sql import SparkSession

from fipe.pipeline.read_configuration import new_columns_df_bronze


@pytest.fixture(scope="session")
def spark_session():
    """
    PyTest fixture for creating a SparkSession.

    This fixture creates a SparkSession and automatically closes it at the end of the test session.
    """
    # Create a SparkSession
    spark = (
        SparkSession.builder.appName("pytest_spark").master("local[2]").getOrCreate()
    )

    # Set any necessary configuration options
    spark.conf.set("spark.sql.shuffle.partitions", "2")

    # Yield the SparkSession to the tests
    yield spark

    # Teardown - stop the SparkSession
    spark.stop()


@pytest.fixture(scope="session")
def config_new_columns_df_bronze():
    """
    PyTest fixture for share the New Coluns between different tests.

    This fixture creates a SparkSession and automatically closes it at the end of the test session.
    """
    return new_columns_df_bronze
