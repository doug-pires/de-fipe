import pytest
from fipe.elt.extract.utils import scrape_options_brands


def test_if_extract_brands():
    # Given the BRANDS within HTML

    class Driver:
        @property
        def page_source(self):
            html_fake = """
            <select data-placeholder="Digite ou selecione a marca do veiculo" class="chosen-select" id="selectMarcacarro" data-no_results_text="Nada encontrado com" data-tipo="marca" style="width: 100%; display: none;" tabindex="-1" data-valor=""><option value="1"></option>Brand 1<option value="1">Brand 2</option></select>
            """
            return html_fake

    driver = Driver()

    # When we call the function to extract ALL BRANDS
    brand_list = scrape_options_brands(driver)

    # Then returns the result into a list and MUST MATCH the expected list
    expected_brands = ["Brand 1", "Brand 2"]

    assert brand_list == expected_brands


if __name__ == "__main__":
    pytest.main(["-v"])
