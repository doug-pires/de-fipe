# This file hold TRANSFORMATION functions

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from typing import List
from fipe.scripts.loggers import get_logger
import pyspark.sql.functions as F


logger = get_logger(__name__)


def transform_list_to_df(
    spark: SparkSession, data: List[str], schema: StructType = None
) -> DataFrame:
    # Create a list of rows with the specified schema
    rows = [(value,) for value in data]
    df = spark.createDataFrame(rows, schema)
    return df


def transform_df_to_list(df: DataFrame):
    # Collect DataFrame rows as a list
    rows_list = df.collect()
    # Convert Row objects to a nested list
    list_data = [list(row) for row in rows_list]
    logger.info("Transforming Dataframe to list")
    return rows_list


def add_column(df: DataFrame, col_name: str, value: str) -> DataFrame:
    df_new_column = df.withColumn(col_name, F.lit(value))
    return df_new_column


if __name__ == "__main__":
    transform_df_to_list()
