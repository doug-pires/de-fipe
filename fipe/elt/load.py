# This file hold SINK functions


from pyspark.sql import DataFrame, SparkSession

from fipe.scripts.loggers import get_logger

logger = get_logger(__name__)


def join_path_table(path: str, delta_table_name: str) -> str:
    """
    Join a Path table.

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


def save_delta_table(
    df: DataFrame,
    path: str,
    delta_table_name: str,
    partition_by: list[str] | None = None,
    mode="overwrite",
    encoding: str = "utf-8",
):
    path_table = join_path_table(path, delta_table_name)
    df.write.format("delta").mode(mode).option("enconding", encoding).partitionBy(
        *partition_by
    ).save(path_table)
    if partition_by is None:
        return logger.info("Saved as Delta table")
    else:
        return logger.info(f"Saved as Delta table partitioned by {partition_by}")


def read_delta_table(
    spark: SparkSession, path: str, delta_table_name: str
) -> DataFrame:
    path_table = join_path_table(path, delta_table_name)
    df = spark.read.format("delta").load(path_table)
    return df


if __name__ == "__main__":
    ...
