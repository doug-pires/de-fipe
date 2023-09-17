from pathlib import Path

from fipe.conf.get_configs import (
    get_base_path,
    get_configs,
    get_schema_from,
    read_config,
)

# Get ALL Webscraper configs


path_conf = Path().cwd() / "fipe/conf"

__webscraper_config = read_config(path_conf / "scraper_config.yml")
# __webscraper_config = get_configs(tag="webscraper")
url: str = __webscraper_config["url"]
xpath_search_car = __webscraper_config["xpaths"]["xpath_search_car"]
xpath_bt_month_year = __webscraper_config["xpaths"]["xpath_bt_month_year"]
xpath_bt_brand = __webscraper_config["xpaths"]["xpath_bt_brand"]
xpath_bt_model = __webscraper_config["xpaths"]["xpath_bt_model"]
xpath_bt_manufacturing_year_fuel = __webscraper_config["xpaths"][
    "xpath_bt_manufacturing_year_fuel"
]
xpath_bt_search = __webscraper_config["xpaths"]["xpath_bt_search"]
xpath_bt_clean_search = __webscraper_config["xpaths"]["xpath_bt_clean_search"]


# Get Bronze Config
__bronze_config = read_config(path_conf / "bronze.yml")

# Get Base Path
bronze_path = get_base_path(__bronze_config)

table_name_bronze = "fipe_bronze"
schema_df_fipe_bronze = get_schema_from(__bronze_config, table_name_bronze)

# Get Mapping Columns for Bronze
new_columns_df_bronze = __bronze_config["df_fipe_bronze_new_columns"]


# # Get Silver Config
# __silver_config = get_configs(tag="silver")

# # Get Base Path
# silver_path = get_base_path(__silver_config)

# schema_df_fipe_silver = get_schema_from(__silver_config, "df_fipe_silver")


if __name__ == "__main__":
    print(__bronze_config)
