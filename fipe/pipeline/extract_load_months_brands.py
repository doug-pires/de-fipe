"""
This module extract all the OPTIONS
available for REFERENCE MONTHS and BRANDS
Save in Bronze Path as Delta Table"""


from fipe.scripts.utils import (
    open_chrome,
    scroll_to_element,
    locate_bt,
    click,
    close_browser,
)
from fipe.scripts.get_config import get_configs, get_schema_from
from fipe.elt.extract.utils import scrape_options_month_year, scrape_options_brands
from fipe.elt.load.utils import transform_list_to_df, save_delta_table, read_delta_table
from fipe.scripts.get_spark import SparkSessionManager

spark_manager = SparkSessionManager(app_name=__name__)
spark = spark_manager.get_spark_session()

# Load Configs
configs = get_configs()

# Get Webscraper configs
webscraper_config = configs["webscraper"]
url: str = webscraper_config["url"]
xpath_search_car = webscraper_config["xpaths"]["xpath_search_car"]

# Get Bronze Config
bronze_config = configs["bronze"]
schema_reference_month = get_schema_from(bronze_config, "reference_month")
schema_brands = get_schema_from(bronze_config, "brands")


def main():
    from dev.dev_utils import path_dev

    site_fipe = open_chrome(url)
    scroll_to_element(site_fipe, xpath_search_car)
    bt = locate_bt(site_fipe, xpath_search_car)
    click(bt)
    months = scrape_options_month_year(site_fipe)
    brands = scrape_options_brands(site_fipe)
    df_months = transform_list_to_df(
        spark=spark, data=months, schema=schema_reference_month
    )
    df_brands = transform_list_to_df(spark=spark, data=brands, schema=schema_brands)
    close_browser(site_fipe)
    save_delta_table(df_months, path_dev, "reference_months")
    save_delta_table(df_brands, path_dev, "brands")
    df_months_as_delta = read_delta_table(spark, path_dev, "reference_months")
    df_months_as_delta.show()


if __name__ == "__main__":
    main()
