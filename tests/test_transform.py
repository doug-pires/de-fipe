import pytest
from fipe.elt.transform.utils import transform_df_to_list
from pyspark.sql.types import StructType, StructField, StringType


def test_if_transform_df_to_list(spark_session):
    # Given the Dataframe Brands
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
    expected_brands = ["Nissan", "Ford", "Chevrolet"]

    assert brand_list == expected_brands


if __name__ == "__main__":
    pytest.main(["-v", "--setup-show"])
