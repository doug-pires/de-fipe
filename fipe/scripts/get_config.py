import pathlib
from pathlib import Path
from typing import Dict, Union

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


# Path to read my configurations
def get_paths_yml_configs():
    pasta_conf = Path().cwd() / "fipe/conf"
    caminhos_arquivos_conf = list(pasta_conf.glob("*.yml"))

    return caminhos_arquivos_conf


def __read_config(path_config: str | Path):
    try:
        config = yaml.safe_load(pathlib.Path(path_config).read_text())
        logger.info("Read the config file")
        return config
    except FileNotFoundError:
        return logger.error("Configuration file not provided")


def get_configs(tag: str):
    """
    Read all configuration files and return it as a dictionary


    Returns:
        Dict: Nested Dict on Configuration
    """
    paths = get_paths_yml_configs()
    configs_list = [__read_config(path) for path in paths]
    configs_dict = {
        config.get("tag"): {k: v for k, v in config.items() if k != "tag"}
        for config in configs_list
    }
    return configs_dict[tag]


def get_schema_from(
    config: dict, dataframe_name: str
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
    configs_bronze = get_configs("bronze")
    configs_silver = get_configs("webscraper")
    print(configs_bronze)
    print(configs_silver)
    # schema = get_schema_from(configs_bronze, dataframe_name="df_fipe_bronze")
