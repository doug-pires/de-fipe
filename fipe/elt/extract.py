# This file hold EXTRACT functions
from bs4 import BeautifulSoup

from fipe.scripts.loggers import get_logger

logger = get_logger(__name__)


def scrape_complete_tbody(driver, new_columns: list[str]) -> dict:
    # Get the new URL
    soup = BeautifulSoup(driver.page_source, "html.parser")

    tbody = soup.find_all("tbody")

    row_tbody = tbody[0]
    td_infos = row_tbody.findChildren("td")

    keys_to_remove = [
        "Mês de referência:",
        "Código Fipe:",
        "Marca:",
        "Modelo:",
        "Ano Modelo:",
        "Autenticação",
        "Data da consulta",
        "Preço Médio",
    ]

    value_tab = [
        " ".join(tr.string.split())
        for tr in td_infos
        if tr.string not in keys_to_remove
    ]

    complete_info = dict(zip(new_columns, value_tab))

    return complete_info


def scrape_options_month_year(driver) -> list[str]:
    """
    This function extracts
    All Month-Year available to iterate over it.
    """

    logger.info("Extracting all Reference Months available")
    # I can pick the values AVAILABLE, or using Requests or BeautifulSoup
    soup = BeautifulSoup(driver.page_source, "html.parser")

    # Month and Year Available
    options_months_years = soup.find_all(
        "select", attrs={"data-tabref": "selectTabelaReferenciacarro"}
    )

    months_years_available = [
        " ".join(month_year.text.split())
        for month_year in options_months_years[0].contents
        if month_year.text != ""
    ]

    return months_years_available


def scrape_options_brands(driver) -> list[str]:
    """
    This function extracts
    All Brands available in the HTML to iterate over it.
    Remove the empty STRINGS -> ""
    """
    logger.info("Extracting all Brands available")
    # I can pick the values AVAILABLE, or using Requests or BeautifulSoup
    soup = BeautifulSoup(driver.page_source, "html.parser")

    # Brands Available
    options_brands = soup.find_all(
        "select",
        attrs={
            "data-placeholder": "Digite ou selecione a marca do veiculo",
            "data-tipo": "marca",
        },
    )

    brands = [
        brand.text.strip()
        for brand in options_brands[0].children
        if brand.text.strip() != "" and brand.text.strip() != " "
    ]

    return brands


def scrape_options_models(driver) -> list[str]:
    logger.info("Extracting all models available")
    # Get the new URL
    soup = BeautifulSoup(driver.page_source, "html.parser")
    options_models_years = soup.find("div", attrs={"class": "step-2"})

    # Get ALL Models available according to a BRAND selected
    models_select = options_models_years.findChildren("select")[0]
    models = models_select.contents

    # My List of Models
    all_models = [model.text for model in models if model.text != ""]
    return all_models


# brand: str, model: str,
def scrape_manufacturing_year_fuel(driver) -> list[str]:
    logger.info("Extracting all MANUFACTURING YEAR - FUEL available")
    # Get the new URL
    soup = BeautifulSoup(driver.page_source, "html.parser")

    year_fuel_for_especific_brand = soup.find(
        "select",
        attrs={"data-placeholder": "Digite ou selecione o ano modelo do veiculo"},
    )

    year_fuel_according_brand_and_model = [
        year.text for year in year_fuel_for_especific_brand if year.text != ""
    ]

    return year_fuel_according_brand_and_model
