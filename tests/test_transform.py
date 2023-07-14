import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from fipe.elt.transform import (
    get_flag_checkpoint,
    transform_df_to_list,
    transform_to_df,
)
from fipe.pipeline.read_configuration import schema_df_fipe_bronze


def test_transform_df_to_list(spark_session):
    # Given the SCHEMA and the DATA to create the Dataframe Brands
    schema = StructType(
        [
            StructField("name", StringType(), nullable=False),
            StructField("age", IntegerType(), nullable=False),
        ]
    )

    data = [
        ("Douglas", 31),
        (
            "Tifa",
            25,
        ),
        ("Marc", 75),
    ]
    df_demo = spark_session.createDataFrame(data=data, schema=schema)

    # When we call the function to transform this DF to list
    person_List = transform_df_to_list(df_demo)

    # Then returns the result into a list and MUST MATCH the expected list
    expected_list = [["Douglas", 31], ["Tifa", 25], ["Marc", 75]]

    assert person_List == expected_list


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
        "reference_month",
        "fipe_code",
        "brand",
        "model",
        "manufacturing_year",
        "authentication",
        "query_date",
        "average_price",
    ]

    assert df_bronze_fipe.columns == expected_columns


def test_flag_checkpoint_returns_true():
    # Given my REFERENCE MONTH and MODEL previously EXTRACTED and the CURRENT reference month and MODEL
    list_saved_in_my_storage = [
        ["junho de 2023", "Model X"],
        ["setembro de 2035", "Model Z"],
    ]
    reference_month = "junho/2023"
    model = "Model X"
    # When I call the function to validate the CHECKPOINT and pass the TWO list
    flag_is_extracted = get_flag_checkpoint(
        current_reference_month=reference_month,
        current_model=model,
        checkpoint_list=list_saved_in_my_storage,
    )

    # Then ASSERT the flag will return TRUE
    assert flag_is_extracted is True


def test_flag_checkpoint_returns_false():
    # Given my REFERENCE MONTH and MODEL previously EXTRACTED and the CURRENT reference month and MODEL
    list_saved_in_my_storage = [
        ["junho de 2023", "Model X"],
        ["setembro de 2035", "Model Z"],
    ]
    reference_month = "dezembro/2023"
    model = "Model T"
    # When I call the function to validate the CHECKPOINT and pass the TWO list
    flag_is_extracted = get_flag_checkpoint(
        current_reference_month=reference_month,
        current_model=model,
        checkpoint_list=list_saved_in_my_storage,
    )

    # Then ASSERT the flag will return TRUE
    assert flag_is_extracted is False


if __name__ == "__main__":
    pytest.main(["-v", "--setup-show", "-s", "-k", "test_df"])
