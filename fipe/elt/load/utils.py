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


def save_as_delta(
    df: DataFrame,
    path: str,
    mode="overwrite",
    schema=None,
):
    return df.write.format("delta").mode(mode).save(path).schema(schema)


def run_vacumm():
    ...


if __name__ == "__main__":
    path_development = Path().cwd()


    schema = StructType([StructField("reference_month", StringType(), False)])
    df = transform_to_df(spark, data=["maio/2023", "abril/2023"], schema=schema)
    save_as_delta(df=df, path=path_development.as_posix())
    df.show()
