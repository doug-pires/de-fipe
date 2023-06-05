import yaml
from pathlib import Path
import pathlib
from typing import Dict, Any
from fipe.scripts.loggers import get_logger
from pyspark.sql.types import *


# Get Logger
# logger = get_logger(__name__)

# Path to read my configurations
path_config = Path().cwd() / "fipe/conf/config_pipeline.yml"


def read_config() -> Dict[str, Any]:
    try:
        config = yaml.safe_load(pathlib.Path(path_config).read_text())
        logger.info("Read the config file")
        return config
    except FileNotFoundError:
        return logger.error("Configuration file not provided")


config = read_config()


def get_schema_from(config: Dict, dataframe_name: str):
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
        df_info = config[dataframe_name]
    except KeyError:
        logger.error("Dataframe name does not exist in the YML file")
        return exit()
    df_columns = df_info["columns"]
    df_names = [column_info["name"] for column_info in df_columns]
    df_types = [eval(column_info["type"]) for column_info in df_columns]
    df_nullable = [column_info.get("nullable", True) for column_info in df_columns]

    # Define the schema for DataFrame4
    fields = [
        StructField(name, data_type, nullable=nullable)
        for name, data_type, nullable in zip(df_names, df_types, df_nullable)
    ]
    schema = StructType(fields)
    logger.info("Dataframe name does not exist in the YML file")
    return schema


# # Testing to create a Dataframe
# # Sample data
# data = [
#     (1, "New York", "John", ["Engineer", "Developer"]),
#     (2, "San Francisco", "Jane", ["Data Scientist"]),
#     (3, "London", "Michael", ["Teacher", "Writer"]),
# ]


# # Create the DataFrame
# df = spark.createDataFrame(data, df_schema)


def get_base_path(key: str):
    try:
        paths = config[key]
        logger.info(f"Key {key} loaded successfully")
    except KeyError:
        logger.error(f"Key {key} does not exist in the YML file")
        return exit()
    return paths


if __name__ == "__main__":
    path = get_base_path("base_paths")
    print(path["bronze"])
