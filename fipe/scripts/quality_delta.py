from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from fipe.elt.load import join_path_table
from fipe.scripts.loggers import get_logger

"""
This module will hold code to OPTIMIZE Delta Tables
- OPTIMIZE
- Z-ORDER
- VACUMM
"""

logger = get_logger(__name__)


def run_vacumm(
    spark: SparkSession, path: str, delta_table_name: str, retention_hours: int
):
    path_table = join_path_table(path, delta_table_name)
    deltaTable = DeltaTable.forPath(spark, path_table)
    try:
        deltaTable.vacuum(retention_hours)
        return logger.info(
            f"Vacuuming the Delta Table {delta_table_name} with a threshold of {retention_hours} hours"
        )
    except Exception:
        return logger.error(
            "Check the configuration: spark.databricks.delta.retentionDurationCheck.enabled is set to true"
        )


def optimize_z_order(spark: SparkSession, path: str, delta_table_name: str):
    """
    Apply OPTIMIZE and
    Apply Z-ORDER in specific COLUMNS
    Z Ordering is an intelligent way to sort your data in files to maximize the file-skipping capabilities of different queries.
    The more files that can be skipped, the faster your queries will run. ( M.Powers )
    Args:
        spark (SparkSession): SparkSession
        path (str): Base Path where is stored the Delta Table
        delta_table_name (str): Provides the Delta Table Name
    """
    path_table = join_path_table(path, delta_table_name)
    deltaTable = DeltaTable.forPath(spark, path_table)
    deltaTable.optimize().executeCompaction()
    logger.info("OPTIMIZED Delta Table")


def get_last_commit_history(spark: SparkSession, path: str, delta_table_name: str):
    path_table = join_path_table(path, delta_table_name)
    deltaTable = DeltaTable.forPath(spark, path_table)
    lastOperationDF = deltaTable.history(1).select(
        "operationMetrics", "operationParameters"
    )
    return lastOperationDF.show(truncate=False)


def check_maintenance():
    """
    This Function search for the Last Maintenance made on the Table
    OPTIMIZE
    Z-ORDER
    VACUUM
    """


if __name__ == "__main__":
    ...
