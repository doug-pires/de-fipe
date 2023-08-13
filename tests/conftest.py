from dataclasses import dataclass, field
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from fipe.conf.read_configuration import (
    new_columns_df_bronze,
    xpath_bt_brand,
    xpath_bt_manufacturing_year_fuel,
    xpath_bt_model,
    xpath_bt_month_year,
    xpath_bt_search,
    xpath_search_car,
)
from fipe.scripts.utils import add_on, click, locate_bt, open_chrome, scroll_to_element


@pytest.fixture(scope="session")
def driver_fixture():
    # Given the PRE-CONDITIONS containing the HTML table tbody
    # Open browser.
    example_values = ["junho/2023", "Nissan", "Sentra GLE", "1995 Gasolina"]

    # Open Webdriver
    driver = open_chrome(url="https://veiculos.fipe.org.br/")

    # Scroll to Button
    scroll_to_element(driver, xpath_search_car)
    # Locate Bt Search Car
    bt_car = locate_bt(driver, xpath_search_car)
    click(bt_car)

    # Locate BT to add Month-Year and Click
    bt_month_year = locate_bt(driver, xpath_bt_month_year)
    click(bt_month_year)
    add_on(bt_month_year, example_values[0])

    # Locate BT to add brand and Click
    bt_brand = locate_bt(driver, xpath_bt_brand)
    click(bt_brand)
    add_on(bt_brand, example_values[1])

    # Locate BT to add model and Click
    bt_model = locate_bt(driver, xpath_bt_model)
    click(bt_model)
    add_on(bt_model, example_values[2])

    # Locate BT to add Manufacturing Year and Fuel to click
    bt_manufacturing_year_fuel = locate_bt(driver, xpath_bt_manufacturing_year_fuel)
    click(bt_manufacturing_year_fuel)
    add_on(bt_manufacturing_year_fuel, example_values[3])

    bt_search = locate_bt(driver, xpath_bt_search)
    click(bt_search)
    yield driver
    driver.close()


@pytest.fixture(scope="session")
def spark_session():
    """
    PyTest fixture for creating a SparkSession.

    This fixture creates a SparkSession and automatically closes it at the end of the test session.
    """
    # Create a SparkSession
    spark = (
        SparkSession.builder.appName("pytest_spark").master("local[2]").getOrCreate()
    )

    # Set any necessary configuration options
    spark.conf.set("spark.sql.shuffle.partitions", "2")

    # Yield the SparkSession to the tests
    yield spark

    # Teardown - stop the SparkSession
    spark.stop()


@pytest.fixture(scope="session")
def config_new_columns_df_bronze():
    """
    PyTest fixture for share the New Coluns between different tests.

    This fixture creates a SparkSession and automatically closes it at the end of the test session.
    """
    return new_columns_df_bronze


@dataclass
class DriverFixture:
    data: str
    page_source: str = field(init=False)

    def __post_init__(self):
        path = Path().cwd() / f"tests/test_data/{self.data}.html"

        with open(path, "r") as file:
            self.page_source = file.read()


@pytest.fixture(scope="session")
def driver_fixture_brands():
    return DriverFixture("brands")


@pytest.fixture(scope="session")
def dummy_data_schema_name():
    fields = [
        StructField("name", StringType(), nullable=False),
        StructField("age", IntegerType(), nullable=False),
    ]
    schema = StructType(fields)

    data = [("Douglas", 31), ("Tifa", 25), ("Marc", 75)]
    return [schema, data]
