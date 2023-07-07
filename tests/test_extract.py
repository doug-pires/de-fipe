import pytest

from fipe.elt.extract import scrape_options_brands


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


def test_if_the_scrape_table_returns_type_dict(table_fipe_fixture):
    # # Given the PRE-CONDITIONS in a FIXTURE containing the fipe table as DICT

    extracted_dict = table_fipe_fixture
    expected_dict = {
        "reference_month": "junho de 2023",
        "fipe_code": "023037-5",
        "brand": "Nissan",
        "model": "Sentra GLE",
        "manufacturing_year": "1995 Gasolina",
        "authentication": "ABC",
        "query_date": "quarta-feira, 28 de junho de 2023 18:34",
        "average_price": "R$ 8.939,00",
    }

    assert extracted_dict == expected_dict
    assert type(extracted_dict) == dict


def test_if_extract_brands_and_return_as_list(driver_fixture_brands):
    # Given the FAKE DRIVER with the PROPERTY page source
    driver = driver_fixture_brands

    # When we call the function to extract ALL BRANDS
    brand_list = scrape_options_brands(driver)

    # Then returns the result into a list and MUST MATCH the expected list
    expected_brands = ["Brand 1", "Brand 2", "Brand 4"]

    assert brand_list == expected_brands


if __name__ == "__main__":
    pytest.main(["-v", "--setup-show", "-k", "extract"])
