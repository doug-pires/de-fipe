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
from fipe.elt.extract.utils import (
    scrape_options_brands,
    scrape_options_models,
    scrape_manufacturing_year_fuel,
    scrape_complete_tbody,
)
from fipe.elt.load.utils import (
    save_delta_table_partitioned,
    read_delta_table,
)
from fipe.elt.transform.utils import (
    transform_list_to_df,
    add_column,
    transform_df_to_list,
)
from fipe.scripts.get_spark import SparkSessionManager
import fipe.pipeline.read_configuration as cf
from fipe.scripts.loggers import get_logger
import time


spark_manager = SparkSessionManager(app_name=__name__)
spark = spark_manager.get_spark_session()

logger = get_logger(__name__)


def main():
    from dev.dev_utils import path_dev

    df_month_year_as_delta = read_delta_table(spark, path_dev, "reference_month")
    list_reference_month_year = transform_df_to_list(df_month_year_as_delta)
    site_fipe = open_chrome(cf.url)
    scroll_to_element(site_fipe, cf.xpath_search_car)
    bt = locate_bt(site_fipe, cf.xpath_search_car)
    click(bt)

    # Start Workflow
    ################
    for month_year in list_reference_month_year[:1]:
        time.sleep(0.5)
        bt_month_year = locate_bt(
            driver=site_fipe,
            xpath=cf.xpath_bt_month_year,
        )
        click(bt_month_year)
        time.sleep(0.5)

        # For Each Reference Month extract all Brands Available
        ################
        list_brands = scrape_options_brands(site_fipe)

        add_on(bt_or_box=bt_month_year, info=month_year)
        for brand in list_brands[:2]:
            time.sleep(1)
            bt_brand = locate_bt(site_fipe, cf.xpath_bt_brand)
            add_on(bt_brand, brand)

            # For Each Brand extract all Models Available
            ################
            list_models = scrape_options_models(site_fipe)

            for model in list_models[:2]:
                bt_model = locate_bt(driver=site_fipe, xpath=cf.xpath_bt_model)
                click(bt_model)
                add_on(bt_model, model)
                # For Each Model extract all Manufacturing Year - Fuel Available
                ################
                list_manufacturing_year_fuel = scrape_manufacturing_year_fuel(site_fipe)
                for manufacturing_year in list_manufacturing_year_fuel[:2]:
                    logger.info(
                        f"Reference Month: {month_year} :-: Brand: {brand} :-: Model: {model} :-: Manufacturing Year: {manufacturing_year}"
                    )
                    time.sleep(1)
                    bt_manufacturing_year = locate_bt(
                        site_fipe, cf.xpath_bt_manufacturing_year_fuel
                    )
                    click(bt_manufacturing_year)
                    add_on(bt_manufacturing_year, manufacturing_year)

                    # Press Search
                    bt_search = locate_bt(site_fipe, cf.xpath_bt_search)
                    click(bt_search)

                    # Extract All Table
                    data = scrape_complete_tbody(site_fipe)
                    print(data)
        # save_delta_table_partitioned()

    close_browser(site_fipe)


if __name__ == "__main__":
    main()
