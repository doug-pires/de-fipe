import sys
from argparse import ArgumentParser
from pathlib import Path
from typing import Any, Dict, Union

import yaml
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from fipe.scripts.loggers import get_logger

# Get Logger
logger = get_logger(__name__)


def get_schema_from(
    config: dict, dataframe_name: str
) -> Union[StructType, StringType, IntegerType, DateType, ArrayType, DecimalType]:
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
    """
    Retrieves the 'base_path' value from a configuration dictionary.

    Args:
        config (Dict): A dictionary containing configuration data.

    Returns:
        str: The 'base_path' value from the configuration dictionary.

    Raises:
        KeyError: If the 'base_path' key does not exist in the configuration dictionary.

    Note:
        This function is used to extract the 'base_path' value from the configuration
        dictionary. If the 'base_path' key is not present, a KeyError is raised,
        and an error message is logged. The function returns the 'base_path' value
        if it exists in the dictionary.

    """
    try:
        base_path = config["base_path"]
        logger.info("Base Path loaded successfully")
    except KeyError:
        logger.error("Base Path key does not exist in the YML file")
        return exit()
    return base_path


def read_config(conf_file: str | Path) -> Dict[str, Any]:
    """
    Reads and parses a configuration file in YAML format.

    Args:
        conf_file (str | Path): The path to the configuration file.

    Returns:
        Dict[str, Any]: A dictionary containing the configuration data.

    Raises:
        FileNotFoundError: If the specified configuration file is not found.

    Note:
        This function uses the `yaml` library to load the configuration data from
        the specified file.

    """
    try:
        config = yaml.safe_load(Path(conf_file).read_text())
        logger.info("Read the config file")
        return config
    except FileNotFoundError:
        logger.error("Configuration file not provided")


def get_conf_file():
    """
    Parses command-line arguments to retrieve the value of the '--conf-file' parameter.

    Returns:
        str | None: The value of the '--conf-file' parameter if provided; otherwise, None.

    """
    p = ArgumentParser()
    p.add_argument("--conf-file", required=False, type=str)
    namespace = p.parse_known_args(sys.argv[1:])[0]
    return namespace.conf_file


def provide_config() -> Dict[str, Any]:
    """
    Provides configuration data by reading it from the '--conf-file' command-line parameter.

    Returns:
        Dict[str, Any]: A dictionary containing the configuration data.

    Note:
        This function first checks if the '--conf-file' parameter has been provided
        through command-line arguments. If provided, it reads the configuration data
        from the specified file using the 'read_config' function. If the parameter
        is not provided, an empty dictionary is returned with a log message.

    """
    logger.info("Reading configuration from --conf-file job option")
    conf_file = get_conf_file()
    if not conf_file:
        logger.info(
            "No conf file was provided, setting configuration to empty dict."
            "Please override configuration in subclass init method"
        )
        return {}
    else:
        logger.info(f"Conf file was provided, reading configuration from {conf_file}")
        return read_config(conf_file)


if __name__ == "__main__":
    path_conf = Path().cwd() / "fipe/conf"

    # get_parameters()
    web_config = read_config(path_conf / "scraper_config.yml")
    print(web_config)
    url = web_config["url"]
    print(url)
    xpath = web_config["xpaths"]["xpath_search_car"]
    print(xpath)
