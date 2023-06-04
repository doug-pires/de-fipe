from pyspark.sql import SparkSession
from dataclasses import dataclass
from delta import configure_spark_with_delta_pip


@dataclass
class SparkSessionManager:
    app_name: str
    additional_options: dict = None
    __spark_session = None

    DEFAULT_DELTA_OPTIONS = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    }

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

        # Add Delta Lake package to the Spark session
        builder_with_delta = configure_spark_with_delta_pip(spark_builder)

        # Add default Delta Lake configurations
        for key, value in self.DEFAULT_DELTA_OPTIONS.items():
            builder_with_delta.config(key, value)

        self.__spark_session = builder_with_delta.getOrCreate()

    def print_config(self):
        print(f"Application Name: {self.app_name}")
        print("Default Delta Lake Options:")
        for key, value in self.DEFAULT_DELTA_OPTIONS.items():
            print(f"  {key}: {value}")

        if self.additional_options:
            print("Additional Options:")
            for key, value in self.additional_options.items():
                print(f"  {key}: {value}")


if __name__ == "__main__":
    additional_options = {
        "spark.master": "local[1]",
        # Add other additional options here
    }
    spark_manager = SparkSessionManager(
        app_name=__name__, additional_options=additional_options
    )
    spark = spark_manager.get_spark_session()
    spark_manager.print_config()
