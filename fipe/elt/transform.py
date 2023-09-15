# This file hold TRANSFORMATION functions

from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.types import StructType

from fipe.elt.load import join_path_table
from fipe.scripts.loggers import get_logger

logger = get_logger(__name__)


def transform_list_to_df(
    spark: SparkSession, data: list[str], schema: StructType = None
) -> DataFrame:
    """
    Transform a list of strings into a DataFrame.

    This function takes a list of strings and converts it into a DataFrame with
    the specified schema. Each string in the input list corresponds to a single
    row in the DataFrame.

    Parameters:
    spark (SparkSession): The Spark session used to create the DataFrame.
    data (List[str]): The list of strings to be transformed into a DataFrame.
    schema (StructType, optional): The schema to be used for the DataFrame.
        If not provided, the resulting DataFrame will have a single string column.

    Returns:
    DataFrame: A DataFrame containing the transformed data.
    """
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


def transform_checkpoint_to_list(
    spark: SparkSession, path: str, delta_table_name: str
) -> list[list[str, str]] | None:
    path_table = join_path_table(path, delta_table_name)
    check_path = Path(path_table)

    if check_path.exists():
        df = spark.read.format("delta").load(path_table)
        df_checkpoint = df.select("reference_month", "model")
        list_checkpoints = transform_df_to_list(df_checkpoint)
        return list_checkpoints
    else:
        list_checkpoints = None
        return list_checkpoints


def add_columns(df: DataFrame, expressions: dict[str:Column]) -> DataFrame:
    """
    Add new columns to a DataFrame based on the provided expressions.

    Parameters:
    - df (DataFrame): The input DataFrame to which new columns will be added.
    - expressions (dict[str, Column]): A dictionary where the keys are column names
      and the values are Column objects representing the expressions for the new columns.

    Returns:
    DataFrame: A new DataFrame with the additional columns.
    """
    cols_name = list(expressions.keys())

    for col in cols_name:
        df = df.withColumn(col, expressions.get(col))

    return df


def flag_is_in_checkpoint(
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

    if checkpoint_list is not None:
        current_list = [current_reference_month.replace("/", " de "), current_model]
        check = current_list in checkpoint_list
        return check
    pass


# UDF to transform string date to first date of the month-year
def parse_month_year(input_str) -> str:
    """
    Transform a string representing a month and year into the first date of that month-year.

    Args:
        input_str (str): A string representing the month and year. The month should be one of the following (case-insensitive):
                        'janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho', 'julho', 'agosto', 'setembro', 'outubro',
                        or 'novembro', 'dezembro'. The year should be a valid numeric representation.

    Returns:
        str: A string representing the first date of the specified month-year in the format 'YYYY-MM-DD'.
            If the input format is invalid or incomplete (missing year or unknown month), the function returns None.
    """
    month_names = {
        "janeiro": "01",
        "fevereiro": "02",
        "março": "03",
        "abril": "04",
        "maio": "05",
        "junho": "06",
        "julho": "07",
        "agosto": "08",
        "setembro": "09",
        "outubro": "10",
        "novembro": "11",
        "dezembro": "12",
    }

    parts = input_str.split(" de ")
    month = month_names.get(parts[0].lower())
    if month is None:
        return None
    try:
        year = parts[1]
    except IndexError:
        return None
    return f"{year}-{month}-01"


def drop_cols(df: DataFrame, cols_to_drop: list[str]) -> DataFrame:
    df = df.drop(*cols_to_drop)
    return df


if __name__ == "__main__":
    print(parse_month_year("agosto de 2025"))

