import pytest
from pyspark.sql import SparkSession


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


class Data:
    def __init__(self, brand) -> None:
        self.brand = brand


brand1 = Data("Toyota")
brand2 = Data("Ford")
brand3 = Data("Chevrolet")
data = [brand1.__dict__, brand2.__dict__, brand3.__dict__]
print(data)
