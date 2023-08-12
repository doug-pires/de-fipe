import pytest
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

from fipe.conf.get_configs import get_schema_from


def test_order_new_columns(config_new_columns_df_bronze):
    # Given the configuration file imported, containing new columns
    current_configurations_columns = config_new_columns_df_bronze
    # When we compare with our expected order
    expected_list = [
        "reference_month",
        "fipe_code",
        "brand",
        "model",
        "manufacturing_year",
        "authentication",
        "query_date",
        "average_price",
    ]

    # Then they should be at the same ORDER and be equals
    assert current_configurations_columns == expected_list
    assert sorted(current_configurations_columns) != expected_list


def test_if_returns_correct_schema_from_configuration_file():
    # Given the Configuration in Dict
    config = {
        "dataframes": {
            "df_test": {
                "columns": [
                    {
                        "name": "date",
                        "type": "DateType()",
                        "nullable": False,
                    },
                    {"name": "first_name", "type": "StringType()", "nullable": True},
                    {"name": "age", "type": "IntegerType()", "nullable": True},
                    {"name": "job", "type": "StringType()", "nullable": True},
                ]
            }
        }
    }

    # When I call the function to extract the schema, must return EQUALS the EXPECTED SCHEMA
    current_schema = get_schema_from(config, "df_test")
    expected_schema = StructType(
        [
            StructField("date", DateType(), False),
            StructField("first_name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("job", StringType(), True),
        ]
    )

    # Then the schema should be the same
    assert current_schema == expected_schema


if __name__ == "__main__":
    pytest.main(["-v", "--setup-show", "-k", "config"])
