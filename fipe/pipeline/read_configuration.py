from fipe.scripts.get_config import get_configs, get_schema_from

# Load Configs
configs = get_configs()

# Get ALL Webscraper configs
__webscraper_config = configs["webscraper"]
url: str = __webscraper_config["url"]
xpath_search_car = __webscraper_config["xpaths"]["xpath_search_car"]
xpath_bt_month_year = __webscraper_config["xpaths"]["xpath_bt_month_year"]
xpath_bt_brand = __webscraper_config["xpaths"]["xpath_bt_brand"]
xpath_bt_model = __webscraper_config["xpaths"]["xpath_bt_model"]
xpath_bt_manufacturing_year_fuel = __webscraper_config["xpaths"][
    "xpath_bt_manufacturing_year_fuel"
]
xpath_bt_search = __webscraper_config["xpaths"]["xpath_bt_search"]


# Get Bronze Config
__bronze_config = configs["bronze"]
schema_df_reference_month = get_schema_from(__bronze_config, "reference_month")
schema_df_fipe_bronze = get_schema_from(__bronze_config, "df_fipe_bronze")


# Get Mapping Columns for Bronze
new_columns_df_bronze = __bronze_config["df_fipe_bronze_new_columns"]


if __name__ == "__main__":
    print(new_columns_df_bronze)
    print(sorted(new_columns_df_bronze))
