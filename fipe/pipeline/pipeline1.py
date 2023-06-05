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
from fipe.scripts.config import read_config, get_schema_from
from fipe.elt.extract.utils import scrape_options_month_year, scrape_options_brands
from fipe.elt.load.utils import transform_list_to_df
from fipe.scripts.get_spark import SparkSessionManager

spark_manager = SparkSessionManager(app_name=__name__)
spark = spark_manager.get_spark_session()

config = read_config()
url: str = config["url"]
xpath_search_car = config["xpaths"]["xpath_search_car"]

schema_reference_month = get_schema_from(config, "reference_month")
schema_brands = get_schema_from(config, "brands")


def main():
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
    df_months.show()
    df_brands.show()


if __name__ == "__main__":
    main()
