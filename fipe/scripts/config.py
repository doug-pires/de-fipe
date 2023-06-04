import yaml
from pathlib import Path
import pathlib
from typing import Dict, Any
from loggers import get_logger
from pyspark.sql.types import *

logger = get_logger(__name__)

# spark_manager = SparkSessionManager(__name__)
# spark = spark_manager.get_spark_session()


path_config = Path().cwd() / "fipe/conf/config_pipeline.yml"


def read_config(conf_file) -> Dict[str, Any]:
    try:
        config = yaml.safe_load(pathlib.Path(conf_file).read_text())
        logger.info("Read the config file")
        return config
    except FileNotFoundError:
        return logger.error("Configuration file not provided")


config = read_config(path_config)


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


if __name__ == "__main__":

    df_schema = get_schema_from(config, dataframe_name="DataFrame4")
    print(df_schema)
