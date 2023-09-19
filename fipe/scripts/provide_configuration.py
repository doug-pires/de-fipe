from fipe.scripts.get_configs import get_base_path, get_schema_from, provide_config

configs = provide_config()

url: str = configs["url"]
xpath_search_car = configs["xpaths"]["xpath_search_car"]
xpath_bt_month_year = configs["xpaths"]["xpath_bt_month_year"]
xpath_bt_brand = configs["xpaths"]["xpath_bt_brand"]
xpath_bt_model = configs["xpaths"]["xpath_bt_model"]
xpath_bt_manufacturing_year_fuel = configs["xpaths"]["xpath_bt_manufacturing_year_fuel"]
xpath_bt_search = configs["xpaths"]["xpath_bt_search"]
xpath_bt_clean_search = configs["xpaths"]["xpath_bt_clean_search"]


# Get Bronze Config
# __bronze_config = read_config(path_demo_bronze)

# Get Base Path
# bronze_path = get_base_path(__bronze_config)
bronze_path = configs["base_path_bronze"]

table_name_bronze = "fipe_bronze"
schema_df_fipe_bronze = get_schema_from(configs, table_name_bronze)

# Get Mapping Columns for Bronze
new_columns_df_bronze = configs["df_fipe_bronze_new_columns"]


# # Get Base Path
# silver_path = get_base_path(__silver_config)

# schema_df_fipe_silver = get_schema_from(__silver_config, "df_fipe_silver")


if __name__ == "__main__":
    print(url)
    print(bronze_path)
