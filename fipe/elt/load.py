# This file hold SINK functions

from pathlib import Path

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

    # Use partitionBy without await
    if partition_by is not None:
        df = (
            df.write.format("delta")
            .mode(mode)
            .option("encoding", encoding)
            .partitionBy(*partition_by)
        )
    else:
        df = df.write.format("delta").mode(mode).option("encoding", encoding)

    # Save the DataFrame as a Delta table
    df.save(path_table)

    # Log the result
    if partition_by is None:
        logger.info("Saved as Delta table")
    else:
        logger.info(f"Saved as Delta table partitioned by {partition_by}")


def read_delta_table(
    spark: SparkSession, path: str, delta_table_name: str
) -> DataFrame:
    path_table = join_path_table(path, delta_table_name)
    check_path = Path(path_table)
    if check_path.exists():
        df = spark.read.format("delta").load(path_table)
        return df
    else:
        logger.error(f"The path provided does not exist: {path_table}")
        raise FileNotFoundError


if __name__ == "__main__":
    ...
