# This file hold TRANSFORMATION functions

from typing import Dict, List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from fipe.scripts.get_spark import SparkSessionManager
from fipe.scripts.loggers import get_logger

logger = get_logger(__name__)


def transform_list_to_df(
    spark: SparkSession, data: List[str], schema: StructType = None
) -> DataFrame:
    # Create a list of rows with the specified schema
    rows = [(value,) for value in data]
    df = spark.createDataFrame(rows, schema)
    return df


def transform_to_df(
    spark: SparkSession, data: List[Dict], schema: StructType = None
) -> DataFrame:
    df = spark.createDataFrame(data, schema)
    return df


def transform_df_to_list(df: DataFrame):
    # Collect DataFrame rows as a list
    rows_list = df.collect()
    # Convert Row objects to a nested list
    brand_list = [row[0] for row in rows_list]
    logger.info("Transforming Dataframe to list")
    return brand_list


def add_column(df: DataFrame, col_name: str, value: str) -> DataFrame:
    df_new_column = df.withColumn(col_name, F.lit(value))
    return df_new_column


def change_all_column_names(df: DataFrame, column_mapping: Dict) -> DataFrame:
    # Create a new DataFrame with renamed columns
    renamed_dataframe = df
    for current_column, new_column in column_mapping.items():
        renamed_dataframe = renamed_dataframe.withColumnRenamed(
            current_column, new_column
        )

    return renamed_dataframe


if __name__ == "__main__":
    import fipe.pipeline.read_configuration as cf

    data = [
        {
            "Mês de referência": "março de 2004",
            "Código Fipe": "038003-2",
            "Marca": "Acura",
            "Modelo": "Integra GS 1.8",
            "Ano Modelo": "1992 Gasolina",
            "Autenticação": "jw754kf5fb",
            "Data da consulta": "quarta-feira, 28 de junho de 2023 18:51",
            "Preço Médio": "R$ 17.393,00",
        },
        {
            "Mês de referência": "março de 2004",
            "Código Fipe": "038003-2",
            "Marca": "Acura",
            "Modelo": "Integra GS 1.8",
            "Ano Modelo": "1991 Gasolina",
            "Autenticação": "jcfl56cfjn",
            "Data da consulta": "quarta-feira, 28 de junho de 2023 18:51",
            "Preço Médio": "R$ 15.958,00",
        },
    ]
    spark = SparkSessionManager(__name__).get_spark_session()
    df = spark.createDataFrame(data=data, schema=cf.schema_df_fipe_bronze)

    df.show()
