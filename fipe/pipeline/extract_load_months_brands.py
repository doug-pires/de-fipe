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
from fipe.elt.extract.utils import scrape_options_month_year, scrape_options_brands
from fipe.elt.load.utils import save_delta_table, read_delta_table
from fipe.elt.transform.utils import transform_list_to_df
from fipe.scripts.get_spark import SparkSessionManager
import fipe.pipeline.read_configuration as cf

spark_manager = SparkSessionManager(app_name=__name__)
spark = spark_manager.get_spark_session()


def main():
    from dev.dev_utils import path_dev

    site_fipe = open_chrome(cf.url)
    scroll_to_element(site_fipe, cf.xpath_search_car)
    bt = locate_bt(site_fipe, cf.xpath_search_car)
    click(bt)
    months = scrape_options_month_year(site_fipe)
    brands = scrape_options_brands(site_fipe)
    df_months = transform_list_to_df(
        spark=spark, data=months, schema=cf.schema_reference_month
    )
    df_brands = transform_list_to_df(spark=spark, data=brands, schema=cf.schema_brands)
    close_browser(site_fipe)
    df_brands.show()
    # save_delta_table(df_months, path_dev, "reference_month")
    # save_delta_table(df_brands, path_dev, "brands")
    # df_months_as_delta = read_delta_table(spark, path_dev, "reference_month")
    # df_months_as_delta.show()


if __name__ == "__main__":
    main()
