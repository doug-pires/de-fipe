"""
This module extract all the OPTIONS
available for REFERENCE MONTHS and BRANDS
Save in Bronze Path as Delta Table"""


import fipe.pipeline.read_configuration as cf
from fipe.elt.extract.utils import scrape_options_month_year
from fipe.elt.load.utils import save_delta_table
from fipe.elt.transform.utils import transform_list_to_df
from fipe.scripts.get_spark import SparkSessionManager
from fipe.scripts.utils import (
    click,
    close_browser,
    locate_bt,
    open_chrome,
    scroll_to_element,
)

spark_manager = SparkSessionManager(app_name=__name__)
spark = spark_manager.get_spark_session()


def main():
    from dev.dev_utils import path_dev

    site_fipe = open_chrome(cf.url)
    scroll_to_element(site_fipe, cf.xpath_search_car)
    bt = locate_bt(site_fipe, cf.xpath_search_car)
    click(bt)
    months = scrape_options_month_year(site_fipe)
    df_months = transform_list_to_df(
        spark=spark, data=months, schema=cf.schema_df_reference_month
    )
    close_browser(site_fipe)
    save_delta_table(df_months, path_dev, "reference_month")


if __name__ == "__main__":
    main()
