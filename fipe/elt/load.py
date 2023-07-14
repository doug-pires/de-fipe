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


def read_checkpoint(
    models_already_extracted: list[str], models_newly_extracted: list[str]
) -> list[str]:
    """
    Get the list of model checkpoints that need to be searched.

    Args:
        models_already_extracted (list[str]): List of models already extracted.
        models_new_extracted (list[str]): List of models newly extracted.

    Returns:
        list[str]: List of model checkpoints to search.

    Examples:
        >>> models_already_extracted = ["A", "B", "C", "D"]
        >>> models_new_extracted = ["B", "D", "E"]
        >>> get_model_checkpoint(models_already_extracted, models_new_extracted)
        ['C', 'A']

    """
    models_to_search = list(set(models_already_extracted) - set(models_newly_extracted))
    return models_to_search


if __name__ == "__main__":
    ...
