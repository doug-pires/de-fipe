# This file hold SINK functions

from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from fipe.scripts.get_spark import SparkSessionManager
from fipe.scripts.loggers import get_logger

logger = get_logger(__name__)


def join_path_table(path: str, delta_table_name: str) -> str:
    """_summary_

    Args:
        path (str): Path in the Storage
        delta_table_name (str):  Delta Table Name

    Returns:
        str: Return a complete Path:
        path_provided/delta_table/_delta_log
        path_provided/delta_table/*.parquet
    """
    path_table = f"{path}/{delta_table_name}"
    return path_table


def save_delta_table(df: DataFrame, path: str, delta_table_name: str, mode="overwrite"):
    path_table = join_path_table(path, delta_table_name)
    df.write.format("delta").mode(mode).save(path_table)
    return logger.info("Saved as Delta table")


def save_delta_table_partitioned(
    df: DataFrame,
    path: str,
    delta_table_name: str,
    partition_by: str,
    mode="overwrite",
):
    path_table = join_path_table(path, delta_table_name)
    df.write.format("delta").mode(mode).partitionBy(partition_by).save(path_table)
    return logger.info(f"Saved as Delta table partitioned by {partition_by}")


def read_delta_table(
    spark: SparkSession, path: str, delta_table_name: str
) -> DataFrame:
    path_table = join_path_table(path, delta_table_name)
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
