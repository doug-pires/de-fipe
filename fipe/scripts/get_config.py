import pathlib
from pathlib import Path
from typing import Any, Dict, Union

import yaml
from pyspark.sql.types import (
    ArrayType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from fipe.scripts.loggers import get_logger

# Get Logger
logger = get_logger(__name__)


def __read_config(path_config) -> Dict[str, Any]:
    try:
        config = yaml.safe_load(pathlib.Path(path_config).read_text())
        logger.info("Read the config file")
        return config
    except FileNotFoundError:
        return logger.error("Configuration file not provided")


# Path to read my configurations
path_scraper_config = Path().cwd() / "fipe/conf/scraper_config.yml"
path_bronze_config = Path().cwd() / "fipe/conf/bronze.yml"
paths_configs_files = [path_scraper_config, path_bronze_config]


def get_configs() -> Dict:
    """Read all configuration files and return it as a dictionary

    Args:
        paths_configurations (List): Paths from the configuration files

    Returns:
        Dict: _description_
    """
    configs_list = [__read_config(path) for path in paths_configs_files]
    configs_dict = {
        config.get("tag"): {k: v for k, v in config.items() if k != "tag"}
        for config in configs_list
    }
    return configs_dict


def get_schema_from(
    config: Dict, dataframe_name: str
) -> Union[StructType, StringType, IntegerType, DateType, ArrayType]:
    """
    In this function,we get the config file from the YML file.
    The schema information is provided as strings, such as StringType() and "ArrayType(StringType())".
    We use eval() to evaluate these strings as Python expressions and obtain the corresponding PySpark data types.

    Args:
        config (Dict): The dict coming from the YML file
        dataframe_name (str): The name of the Dataframe to get the information such as Name of the Columns and Schema

    Returns:
        StructType: StrucType from Spark.
    """
    try:
        get_dataframe_key = config["dataframes"]
        df_info = get_dataframe_key[dataframe_name]
    except Exception:
        logger.error("Dataframe name does not exist in the YML file")
        return exit()
    df_columns = df_info["columns"]
    df_names = [column_info["name"] for column_info in df_columns]
    df_types = [eval(column_info["type"]) for column_info in df_columns]
    df_nullable = [column_info.get("nullable", True) for column_info in df_columns]

    fields = [
        StructField(name, data_type, nullable=nullable)
        for name, data_type, nullable in zip(df_names, df_types, df_nullable)
    ]
    schema = StructType(fields)
    logger.info(f"Loaded schema for {dataframe_name} dataframe")
    return schema


def get_base_path(config: Dict) -> str:
    try:
        base_path = config["base_path"]
        logger.info("Base Path loaded successfully")
    except KeyError:
        logger.error("Base Path key does not exist in the YML file")
        return exit()
    return base_path


if __name__ == "__main__":
    configs = get_configs()
    mapping = configs["bronze"]["df_fipe_bronze_new_columns"]

    print(mapping)
    #  base_bronze_path = get_base_path(config_bronze)
    # schema = get_schema_from(config_bronze, "brands")
