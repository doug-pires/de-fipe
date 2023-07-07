import pytest
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

from fipe.scripts.get_config import get_configs, get_schema_from


def test_returns_correct_schema():
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
