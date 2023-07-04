import pytest

from fipe.elt.extract.utils import scrape_complete_tbody, scrape_options_brands


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


def test_if_the_scrape_table_returns_type_dict(config_new_columns_df_bronze):
    # Given the table into the HTML tbody TAG within HTML

    current_configurations_columns = config_new_columns_df_bronze

    class Driver:
        @property
        def page_source(self):
            html_fake = """<tbody>
                    <tr>
                        <td class="noborder"><p>Mês de referência:</p></td>
                        <td><p>junho de 2023 </p></td>
                    </tr>
                    <tr>
                        <td class="noborder"><p>Código Fipe:</p></td>
                        <td><p>060003-2</p></td>
                    </tr>
                    <tr>
                        <td class="noborder"><p>Marca:</p></td>
                        <td><p>VolksWagen</p></td>
                    </tr>
                    <tr>
                        <td class="noborder"><p>Modelo:</p></td>
                        <td><p>Gol</p></td>
                    </tr>
                    <tr>
                        <td class="noborder"><p>Ano Modelo:</p></td>
                        <td><p>2010 Diesel</p></td>
                    </tr>
                    <tr>
                        <td class="noborder"><p>Autenticação</p></td>
                        <td><p>ABC</p></td>
                    </tr>
                    <tr>
                        <td class="noborder"><p>Data da consulta</p></td>
                        <td><p>quarta-feira, 28 de junho de 2023 18:34</p></td>
                    </tr>
                    <tr class="last">
                        <td class="noborder"><p>Preço Médio</p></td>
                        <td><p>R$ 64.440,00</p></td>
                    </tr>
                </tbody>"""
            return html_fake

    driver = Driver()
    # When we call the function to extract ALL BRANDS
    table_as_dict = scrape_complete_tbody(driver, current_configurations_columns)

    # Then returns the result into a list and MUST MATCH the expected list
    expected_dict = {
        "reference_month": "junho de 2023",
        "fipe_code": "060003-2",
        "brand": "VolksWagen",
        "model": "Gol",
        "manufacturing_year": "2010 Diesel",
        "authentication": "ABC",
        "query_date": "quarta-feira, 28 de junho de 2023 18:34",
        "average_price": "R$ 64.440,00",
    }

    assert table_as_dict == expected_dict
    assert type(table_as_dict) == dict


def test_if_extract_brands_and_return_as_list():
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
    pytest.main(["-v", "--setup-show", "-k", "extract"])
