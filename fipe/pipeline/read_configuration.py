from fipe.scripts.get_config import get_configs, get_schema_from

# Load Configs
configs = get_configs()

# Get ALL Webscraper configs
webscraper_config = configs["webscraper"]
url: str = webscraper_config["url"]
xpath_search_car = webscraper_config["xpaths"]["xpath_search_car"]
xpath_bt_month_year = webscraper_config["xpaths"]["xpath_bt_month_year"]
xpath_bt_brand = webscraper_config["xpaths"]["xpath_bt_brand"]
xpath_bt_model = webscraper_config["xpaths"]["xpath_bt_model"]
xpath_bt_manufacturing_year_fuel = webscraper_config["xpaths"][
    "xpath_bt_manufacturing_year_fuel"
]
xpath_bt_search = webscraper_config["xpaths"]["xpath_bt_search"]


# Get Bronze Config
bronze_config = configs["bronze"]
schema_models = get_schema_from(bronze_config, "models")
schema_manufacturing_year_fuels = get_schema_from(
    bronze_config, "manufacturing_year_fuel"
)

if __name__ == "__main__":
    print(schema_models)
