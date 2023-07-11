import pytest
from pyspark.sql.types import StringType, StructField, StructType

from fipe.elt.transform import transform_df_to_list, transform_to_df
from fipe.pipeline.read_configuration import schema_df_fipe_bronze


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
            "reference_month": "março de 2004",
            "fipe_code": "038003-2",
            "brand": "Acura",
            "model": "Integra GS 1.8",
            "manufacturing_year": "1992 Gasolina",
            "authentication": "jw754kf5fb",
            "query_date": "quarta-feira, 28 de junho de 2023 18:51",
            "average_price": "R$ 17.393,00",
        },
        {
            "reference_month": "março de 2004",
            "fipe_code": "038003-2",
            "brand": "Acura",
            "model": "Integra GS 1.8",
            "manufacturing_year": "1991 Gasolina",
            "authentication": "jcfl56cfjn",
            "query_date": "quarta-feira, 28 de junho de 2023 18:51",
            "average_price": "R$ 15.958,00",
        },
    ]

    # When call the function to transform to a DF
    df_bronze_fipe = transform_to_df(
        spark_session, data=list_of_dicts, schema=schema_df_fipe_bronze
    )

    # Then returns me a DF with these columns and assert these columns in my expcted list
    expected_columns = [
        "reference_month_2",
        "fipe_code",
        "brand",
        "model",
        "manufacturing_year",
        "authentication",
        "query_date",
        "average_price",
    ]

    assert df_bronze_fipe.columns == expected_columns


if __name__ == "__main__":
    pytest.main(["-v", "--setup-show", "-s", "-k", "test_df"])
