"""
This module according to BRAND extract all MODELS and MANUFACTURING YEAR / FUEL
Save in Bronze Path
"""
import time

from fipe.elt.extract import (
    scrape_complete_tbody,
    scrape_manufacturing_year_fuel,
    scrape_options_brands,
    scrape_options_models,
)
from fipe.elt.load import read_delta_table, save_delta_table_partitioned
from fipe.elt.transform import transform_df_to_list, transform_to_df
from fipe.pipeline.read_configuration import (
    new_columns_df_bronze,
    schema_df_fipe_bronze,
    url,
    xpath_bt_brand,
    xpath_bt_clean_search,
    xpath_bt_manufacturing_year_fuel,
    xpath_bt_model,
    xpath_bt_month_year,
    xpath_bt_search,
    xpath_search_car,
)
from fipe.scripts.get_spark import SparkSessionManager
from fipe.scripts.loggers import get_logger
from fipe.scripts.utils import (
    add_on,
    click,
    close_browser,
    locate_bt,
    open_chrome,
    scroll_to_element,
)

logger = get_logger(__name__)


def main():
    from dev.dev_utils import path_dev

    spark_manager = SparkSessionManager(app_name=__name__)
    spark = spark_manager.get_spark_session()

    df_month_year_as_delta = read_delta_table(spark, path_dev, "reference_month")
    list_reference_month_year = transform_df_to_list(df_month_year_as_delta)
    site_fipe = open_chrome(url, False)
    scroll_to_element(site_fipe, xpath_search_car)
    bt = locate_bt(site_fipe, xpath_search_car)
    click(bt)

    # Start Workflow
    ################
    start_time = time.time()
    for month_year in list_reference_month_year[:2]:
        time.sleep(0.5)
        bt_month_year = locate_bt(
            driver=site_fipe,
            xpath=xpath_bt_month_year,
        )
        click(bt_month_year)

        # Define List will keep the dictionaries of Data
        list_of_dicts = []
        add_on(bt_or_box=bt_month_year, info=month_year)

        # For Each Reference Month extract all Brands Available
        ################
        list_brands = scrape_options_brands(site_fipe)

        for brand in list_brands:
            time.sleep(0.5)
            bt_brand = locate_bt(site_fipe, xpath_bt_brand)
            add_on(bt_brand, brand)

            # For Each Brand extract all Models Available
            ################
            list_models = scrape_options_models(site_fipe)

            for model in list_models:
                scroll_to_element(driver=site_fipe, xpath=xpath_bt_model)
                bt_model = locate_bt(driver=site_fipe, xpath=xpath_bt_model)
                click(bt_model)
                add_on(bt_model, model)

                # For Each Model extract all Manufacturing Year - Fuel Available
                ################
                list_manufacturing_year_fuel = scrape_manufacturing_year_fuel(site_fipe)
                logger.info(
                    f"Manufacturing Year available: {list_manufacturing_year_fuel} for {month_year} and brand {brand}, model: {model}"
                )
                for manufacturing_year in list_manufacturing_year_fuel:
                    bt_manufacturing_year = locate_bt(
                        site_fipe, xpath_bt_manufacturing_year_fuel
                    )
                    click(bt_manufacturing_year)
                    add_on(bt_manufacturing_year, manufacturing_year)

                    # Press Search
                    # Sometimes this will be searching for the Table, then we will try to skip if fails
                    bt_search = locate_bt(site_fipe, xpath_bt_search)
                    click(bt_search)
                    time.sleep(0.5)

                    # Extract All Table
                    data = scrape_complete_tbody(site_fipe, new_columns_df_bronze)

                    list_of_dicts.append(data)

                # Clean Search
                # We are cleaning because some model are not available for the SPECIFIC Manufacturing Year - Fuel left by the last process.
                # The options were REFRESH the PAGE OR Clean the fields EXCEPT the reference_month.
                # That's why I decided clean the table and add the BRAND again.
                bt_clean = locate_bt(site_fipe, xpath_bt_clean_search)
                click(bt_clean)
                time.sleep(0.5)

                # After Clean, lWe will add again the Brand.
                click(bt_brand)
                add_on(bt_brand, brand)

        df = transform_to_df(spark, list_of_dicts, schema_df_fipe_bronze)
        save_delta_table_partitioned(
            df=df,
            path=path_dev,
            mode="append",
            delta_table_name="fipe_bronze",
            partition_by=["reference_month", "brand", "model"],
        )

    list_of_dicts.clear()

    close_browser(site_fipe)
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Took {round(total_time,2)} seconds")


if __name__ == "__main__":
    main()
