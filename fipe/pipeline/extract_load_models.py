"""
This module  according to BRAND extract all MODELS and MANUFACTURING YEAR / FUEL
Save in Bronze Path
"""
from fipe.scripts.utils import (
    open_chrome,
    scroll_to_element,
    locate_bt,
    click,
    add_on,
    close_browser,
)
from fipe.scripts.get_config import get_configs, get_schema_from
from fipe.elt.extract.utils import scrape_options_models, scrape_manufacturing_year_fuel
from fipe.elt.load.utils import (
    transform_list_to_df,
    transform_df_to_list,
    save_delta_table,
    save_delta_table_partitioned,
    read_delta_table,
    add_column,
)
from fipe.scripts.get_spark import SparkSessionManager
import time

spark_manager = SparkSessionManager(app_name=__name__)
spark = spark_manager.get_spark_session()

# Load Configs
configs = get_configs()

# Get Webscraper configs
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


def main():
    from dev.dev_utils import path_dev

    df_month_year_as_delta = read_delta_table(spark, path_dev, "reference_months")
    df_brands = read_delta_table(spark, path_dev, "brands")
    list_reference_month_year = transform_df_to_list(df_month_year_as_delta)
    list_brands = transform_df_to_list(df_brands)
    site_fipe = open_chrome(url)
    scroll_to_element(site_fipe, xpath_search_car)
    bt = locate_bt(site_fipe, xpath_search_car)
    click(bt)
    for month_year in list_reference_month_year[:1]:
        time.sleep(0.5)
        bt_month_year = locate_bt(
            driver=site_fipe,
            xpath=xpath_bt_month_year,
        )
        click(bt_month_year)
        time.sleep(0.5)
        add_on(bt_or_box=bt_month_year, info=month_year)
        for brand in list_brands[:3]:
            time.sleep(1)
            bt_brand = locate_bt(site_fipe, xpath_bt_brand)
            add_on(bt_brand, brand)
            list_models = scrape_options_models(site_fipe)
            print(list_models)
            df_models = transform_list_to_df(spark, list_models, schema_models)
            df_with_brand = add_column(df_models, "brand", brand[0])
            save_delta_table_partitioned(
                df=df_with_brand,
                path=path_dev,
                delta_table_name="models",
                partition_by="brand",
            )
            for model in list_models[:2]:
                bt_model = locate_bt(driver=site_fipe, xpath=xpath_bt_model)
                click(bt_model)
                add_on(bt_model, model)
                list_manufacturing_year_fuel = scrape_manufacturing_year_fuel(site_fipe)
                print(
                    f"Model : {model}, Manufacturing Year and kind of fuel: {list_manufacturing_year_fuel}"
                )

    close_browser(site_fipe)


if __name__ == "__main__":
    main()
