# This file hold TRANSFORMATION functions


import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from fipe.scripts.loggers import get_logger

logger = get_logger(__name__)


def transform_list_to_df(
    spark: SparkSession, data: list[str], schema: StructType = None
) -> DataFrame:
    # Create a list of rows with the specified schema
    rows = [(value,) for value in data]
    df = spark.createDataFrame(rows, schema)
    return df


def transform_to_df(
    spark: SparkSession, data: list[dict[str, str]], schema: StructType = None
) -> DataFrame:
    df = spark.createDataFrame(data, schema)
    return df


def transform_df_to_list(df: DataFrame) -> list[str] | list[list[str, str]]:
    """
    Transforms a DataFrame into a nested list representation.

    Args:
        df (DataFrame): The DataFrame to be transformed.

    Returns:
        list[str] | list[list[str, str]]: The transformed DataFrame represented as a nested list.
            - If the DataFrame has a single column, a flat list of column values is returned.
            - If the DataFrame has multiple columns, a nested list of row values, where each row
              is represented as a list of column values, is returned.

    Example:
        >>> df = DataFrame([("John", 25), ("Alice", 30)], columns=["Name", "Age"])
        >>> transform_df_to_list(df)
        [['John', '25'], ['Alice', '30']]
    """
    # Collect DataFrame rows as a list
    rows_list = df.collect()
    # Convert Row objects to a nested list
    col_list = [list(row) for row in rows_list]
    logger.info("Transforming Dataframe to list")
    return col_list


def add_column(df: DataFrame, col_name: str, value: str) -> DataFrame:
    df_new_column = df.withColumn(col_name, F.lit(value))
    return df_new_column


def get_flag_checkpoint(
    current_reference_month: str,
    current_model: str,
    checkpoint_list: list[list[str, str]],
) -> bool:
    """The function will help VALIDATE if we already extracted this MODEL in a SPECIFIC REFERENCE MONTH

    Args:
        current_reference_month (str): The CURRENT REFERENCE MONTH in the LOOP
        current_model (str): The CURRENT MODEL in the LOOP
        checkpoint_list (list[list[str, str]]): The list of REFERENCE MONTH and MODEL I have already extracted.
        Considered REFERENCE MONTH and MODEL, because I need to consider BOTH.
    Returns:
        bool: A FLAG basically saying "ALREADY extracted" go to another MODEL.
    """
    current_list = [current_reference_month.replace("/", " de "), current_model]
    check = current_list in checkpoint_list
    return check


if __name__ == "__main__":
    ...
