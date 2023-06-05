from delta.tables import DeltaTable
from pyspark.sql import SparkSession
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
    path_table = f"{path}/{delta_table_name}"
    deltaTable = DeltaTable.forPath(spark, path_table)
    try:
        deltaTable.vacuum(retention_hours)
        return logger.info(
            f"Vacuuming the Delta Table {delta_table_name} with a threshold of {retention_hours} hours"
        )
    except Exception:
        return logger.error(
            "Check the configuration: spark.databricks.delta.retentionDurationCheck.enabled is set to false"
        )


def z_order(spark: SparkSession, path: str, delta_table_name: str):
    ...


if __name__ == "__main__":
    ...
