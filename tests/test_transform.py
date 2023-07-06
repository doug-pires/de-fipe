import pytest
from pyspark.sql.types import StringType, StructField, StructType

import fipe.pipeline.read_configuration as cf
from fipe.elt.transform.transform import (
    change_all_column_names,
    transform_df_to_list,
    transform_to_df,
)


def test_if_transform_df_to_list(spark_session):
    # Given the SCHEMA and the DATA to create the Dataframe Brands
    schema = StructType(
        [
            StructField("brand", StringType(), nullable=False),
        ]
    )

    data = [("Toyota",), ("Ford",), ("Chevrolet",)]
    df_brands = spark_session.createDataFrame(data=data, schema=schema)

    # When we call the function to extract ALL BRANDS
    brand_list = transform_df_to_list(df_brands)

    # Then returns the result into a list and MUST MATCH the expected list
    expected_brands = ["Toyota", "Ford", "Chevrolet"]

    assert brand_list == expected_brands


def test_transform_list_of_dicts_to_df(spark_session):
    # Given the List Of Dicts and the SCHEMA for them
    list_of_dicts = [
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

    # When call the function to transform to a DF
    df_bronze_fipe = transform_to_df(
        spark_session, data=list_of_dicts, schema=cf.schema_df_fipe_bronze
    )

    # Then returns me a DF with these columns and assert these columns in my expcted list
    expected_columns = [
        "Mês de referência",
        "Código Fipe",
        "Marca",
        "Modelo",
        "Ano Modelo",
        "Autenticação",
        "Data da consulta",
        "Preço Médio",
    ]

    assert df_bronze_fipe.columns == expected_columns


def test_change_all_coluns_names_according_to_mapping(spark_session):
    # Given this DataFrame
    # Given the List Of Dicts and the SCHEMA for them
    list_of_dicts = [
        {
            "Primeiro Nome": "Douglas",
            "Segundo Nome": "Pires",
            "Profissão": "Engenheiro de Dados",
        },
        {
            "Primeiro Nome": "Maria Vitoria",
            "Segundo Nome": "Alves",
            "Profissão": "Dentista",
        },
    ]

    schema = StructType(
        [
            StructField("Primeiro Nome", StringType(), nullable=False),
            StructField("Segundo Nome", StringType(), nullable=False),
            StructField("Profissão", StringType(), nullable=False),
        ]
    )

    df_bronze_fipe = spark_session.createDataFrame(data=list_of_dicts, schema=schema)
    mapping = {
        "Primeiro Nome": "first_name",
        "Segundo Nome": "surname",
        "Profissão": "job",
    }
    # When I call the function to change the name of all COLUMNS to a MAPPING table
    new_df = change_all_column_names(df=df_bronze_fipe, column_mapping=mapping)
    expected_columns = ["first_name", "surname", "job"]

    assert new_df.columns == expected_columns


if __name__ == "__main__":
    pytest.main(["-v", "--setup-show", "-k", "transform"])
