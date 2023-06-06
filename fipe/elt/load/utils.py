# This file hold SINK functions

from pyspark.sql import SparkSession
from fipe.scripts.get_spark import SparkSessionManager
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from typing import List
from fipe.scripts.loggers import get_logger

logger = get_logger(__name__)


def transform_list_to_df(
    spark: SparkSession, data: List[str], schema: StructType = None
) -> DataFrame:
    # Create a list of rows with the specified schema
    rows = [(value,) for value in data]
    df = spark.createDataFrame(rows, schema)
    return df


def save_delta_table(df: DataFrame, path: str, delta_table_name: str, mode="overwrite"):
    path_table = f"{path}/{delta_table_name}"
    logger.info("Saved as Delta table")
    return df.write.format("delta").mode(mode).save(path_table)


def read_delta_table(
    spark: SparkSession, path: str, delta_table_name: str
) -> DataFrame:
    path_table = f"{path}/{delta_table_name}"
    df = spark.read.format("delta").load(path_table)
    return df


if __name__ == "__main__":
    from dev.dev_utils import path_dev

    additional_options = {
        "spark.master": "local[*]",
        "spark.databricks.delta.retentionDurationCheck.enabled": "false",
    }
    spark_manager = SparkSessionManager(
        app_name=__name__, additional_options=additional_options
    )
    spark = spark_manager.get_spark_session()
    spark_manager.print_config()

    schema = StructType([StructField("reference_month", StringType(), False)])
    df = transform_list_to_df(spark, data=["maio/2023", "abril/2023"], schema=schema)
    save_delta_table(df=df, path=path_dev, delta_table_name="months")
