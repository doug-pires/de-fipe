# This file hold SINK functions

from pyspark.sql import SparkSession
from fipe.scripts.get_spark import SparkSessionManager
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from typing import List
from pathlib import Path


def transform_to_df(
    spark: SparkSession, data: List[str], schema: StructType = None
) -> DataFrame:
    # Create a list of rows with the specified schema
    rows = [(value,) for value in data]
    df = spark.createDataFrame(rows, schema)
    return df


def save_as_delta(df: DataFrame, path: str, delta_table_name: str, mode="overwrite"):
    path_table = f"{path}/{delta_table_name}"
    return df.write.format("delta").mode(mode).save(path_table)


def run_vacumm():
    ...


if __name__ == "__main__":
    additional_options = {
        "spark.master": "local[*]",
        # Add other additional options here
    }
    spark_manager = SparkSessionManager(
        app_name=__name__, additional_options=additional_options
    )
    spark = spark_manager.get_spark_session()
    spark_manager.print_config()

    # Path Development
    path_development = Path().cwd().as_posix()
    schema = StructType([StructField("reference_month", StringType(), False)])
    df = transform_to_df(spark, data=["maio/2023", "abril/2023"], schema=schema)
    save_as_delta(df=df, path=path_development, delta_table_name="months")
    df.show()
    print(path_development)
