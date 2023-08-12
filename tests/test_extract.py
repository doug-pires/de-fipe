import logging

import pytest

from fipe.elt.extract import (
    scrape_complete_tbody,
    scrape_options_brands,
    scrape_options_models,
    scrape_options_month_year,
)
from fipe.scripts.utils import retry_search


def test_if_extract_reference_month_and_return_as_list(driver_fixture):
    # Given the DRIVER, we need to extract the MONTH-YEAR
    driver = driver_fixture

    # When we call the function to extract ALL BRANDS
    list_reference_months = scrape_options_month_year(driver)

    # Then returns the result into a list and MUST MATCH the expected list
    expected_reference_months = ["junho/2023", "maio/2022", "dezembro/2021"]

    for month_year in expected_reference_months:
        assert month_year in list_reference_months


def test_if_extract_brand_and_return_as_list(driver_fixture):
    # Given the DRIVER, we need to extract the brands
    driver = driver_fixture

    # When we call the function to extract ALL BRANDS
    brand_list = scrape_options_brands(driver)

    # Then returns the result into a list and MUST MATCH the expected list
    expected_brands = ["Acura", "Nissan", "Alfa Romeo", "Ford", "Toyota"]

    for brand in expected_brands:
        assert brand in brand_list


def test_if_extract_model_according_to_a_specific_brand_and_return_as_list(
    driver_fixture,
):
    # Given the DRIVER, FILLED with the BRAND, Nissan, at least Sentra GLE, Frontier must be in the List
    driver = driver_fixture

    # When we call the function to extract ALL BRANDS
    expected_models = scrape_options_models(driver)

    # Then returns the result into a list and MUST MATCH the expected list
    expected_models = ["Sentra GLE", "Altima SE 2.4 16V", "Infinit 3.0", "Micra 1.0"]

    for models in expected_models:
        assert models in expected_models


def test_if_the_scrape_table_returns_type_dict(
    driver_fixture, config_new_columns_df_bronze
):
    # Given the PRE-CONDITIONS in a FIXTURE containing the fipe table as DICT
    new_columns_df_bronze = config_new_columns_df_bronze
    table_fipe_as_dict = scrape_complete_tbody(driver_fixture, new_columns_df_bronze)

    # Since it's DYNAMICALLY the field AUTEHNTICATION
    # The Query Date will be hard to match, we will HARDCODE these values.

    table_fipe_as_dict["authentication"] = "ABC"
    table_fipe_as_dict["query_date"] = "quarta-feira, 28 de junho de 2023 18:34"

    expected_dict = {
        "reference_month": "junho de 2023",
        "fipe_code": "023037-5",
        "brand": "Nissan",
        "model": "Sentra GLE",
        "manufacturing_year_fuel": "1995 Gasolina",
        "authentication": "ABC",
        "query_date": "quarta-feira, 28 de junho de 2023 18:34",
        "average_price": "R$ 8.939,00",
    }

    assert table_fipe_as_dict == expected_dict
    assert type(table_fipe_as_dict) == dict


def test_retry_search_logs(caplog):
    # Set up logging
    caplog.set_level(logging.ERROR)  # Set the log level to capture critical messages

    # Create a dummy function that always raises an exception DECORATED by retry
    @retry_search(max_attempts=2, delay=1)
    def dummy_function():
        raise ValueError("Something went wrong")

    # Call the decorated function
    dummy_function()
    # Assert that the expected log messages were generated
    expected_logs = [
        "Attempt 1 failed to connect",
        "Attempt 2 failed to connect",
        "Data extraction stopped, because Function dummy_function failed after 2 attempts",
    ]

    log_messages = [msg.message for msg in caplog.records]

    for expected_log in expected_logs:
        assert expected_log in log_messages


if __name__ == "__main__":
    pytest.main(["-v", "-s", "--setup-show", "-k", "test_retry_search_logs"])
