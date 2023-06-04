from pyspark.sql import SparkSession
from dataclasses import dataclass
import logging


@dataclass
class SparkManager:
    spark: SparkSession = None
    app_name: str = None

    @property
    def get_spark_session(self) -> SparkSession:
        """
        Returns:
            SparkSession: checks if the Spark session has been created. If not,
            it creates a new session using the SparkSession builder and stores it in the spark attribute.
            If the session has already been created, it simply returns the existing session.
        """
        if self.spark is None:
            self.spark = SparkSession.builder.appName(self.app_name).getOrCreate()
        return self.spark

    def disable_logs(self):
        logger = logging.getLogger("py4j")
        logger.setLevel(logging.ERROR)


@dataclass
class SparkSessionManager:
    app_name: str
    additional_options: dict = None
    __spark_session = None

    def get_spark_session(self):
        """Returns a Spark Session

        Returns:
            spark: SparkSession
        """
        if self.__spark_session is None:
            self.__create_spark_session()
        return self.__spark_session

    def __create_spark_session(self):
        spark_builder = SparkSession.builder.appName(self.app_name)

        if self.additional_options:
            for key, value in self.additional_options.items():
                spark_builder.config(key, value)

        self.__spark_session = spark_builder.getOrCreate()

    def print_options(self):
        print(f"Application Name: {self.app_name}")
        if self.additional_options:
            print("Additional Options:")
            for key, value in self.additional_options.items():
                print(f"  {key}: {value}")
