"""
This module according to BRAND extract all MODELS and MANUFACTURING YEAR / FUEL
Save in Bronze Path
"""

import json
import time
from datetime import datetime

from databricks.sdk import WorkspaceClient

from fipe.elt.extract import scrape_complete_tbody
from fipe.scripts.loggers import get_logger
from fipe.scripts.provide_configuration import (
    new_columns_df_bronze,
    url,
    xpath_bt_brand,
    xpath_bt_clean_search,
    xpath_bt_manufacturing_year_fuel,
    xpath_bt_model,
    xpath_bt_month_year,
    xpath_bt_search,
    xpath_search_car,
)
from fipe.scripts.utils import (
    add_on,
    click,
    close_browser,
    locate_bt,
    open_chrome,
    scroll_to_element,
)

logger = get_logger(__name__)


list_reference_months = ["janeiro/2023", "fevereiro/2023"]
list_brands = ["Nissan"]
list_models = ["NX 2000"]
list_manufacturing_year_fuel = ["1994 Gasolina"]


def main():
    # spark_manager = SparkSessionManager(app_name=__name__)
    # spark = spark_manager.get_spark_session()

    # # Read Checkpoint
    # list_checkpoints = transform_checkpoint_to_list(
    #     spark=spark, path=bronze_path, delta_table_name="fipe_bronze"
    # )

    site_fipe = open_chrome(url=url, headless=True)
    scroll_to_element(site_fipe, xpath_search_car)
    bt = locate_bt(site_fipe, xpath_search_car)
    click(bt)

    # Start Workflow
    ################

    # list_reference_months = scrape_options_month_year(site_fipe)

    # It will hold my fipe table
    list_fipe_information: list = []
    for month_year in list_reference_months:
        # time.sleep(0.5)

        bt_month_year = locate_bt(
            driver=site_fipe,
            xpath=xpath_bt_month_year,
        )
        click(bt_month_year)
        add_on(bt_or_box=bt_month_year, info=month_year)

        # For Each Reference Month extract all Brands Available
        ################
        # list_brands = scrape_options_brands(site_fipe)

        for brand in list_brands:
            time.sleep(0.5)
            bt_brand = locate_bt(site_fipe, xpath_bt_brand)
            add_on(bt_brand, brand)

            # For Each Brand extract all Models Available
            ################
            # list_models = scrape_options_models(site_fipe)

            for model in list_models:
                # is_downloaded = flag_is_in_checkpoint(
                #     current_reference_month=month_year,
                #     current_model=model,
                #     checkpoint_list=list_checkpoints,
                # )
                # print(
                #     f"Was downloaded info for reference month {month_year} and the model {model} yet? {is_downloaded}"
                # )
                # if not is_downloaded:
                #     # print("Downloaded yet... Leaving!")
                #     # break

                scroll_to_element(driver=site_fipe, xpath=xpath_bt_model)
                bt_model = locate_bt(driver=site_fipe, xpath=xpath_bt_model)
                click(bt_model)
                add_on(bt_model, model)

                # For Each Model extract all Manufacturing Year - Fuel Available
                ################
                # list_manufacturing_year_fuel = scrape_manufacturing_year_fuel(site_fipe)
                logger.info(
                    f"Manufacturing Year available: {list_manufacturing_year_fuel} for {month_year} and brand {brand}, model: {model}"
                )

                for manufacturing_year in list_manufacturing_year_fuel[0:2]:
                    bt_manufacturing_year = locate_bt(
                        site_fipe, xpath_bt_manufacturing_year_fuel
                    )
                    click(bt_manufacturing_year)
                    add_on(bt_manufacturing_year, manufacturing_year)

                    bt_search = locate_bt(site_fipe, xpath_bt_search)
                    # Press Search
                    # We decorated that function to RETRY whenever we can not access the table
                    click(bt_search)

                    # Extract All Table
                    data = scrape_complete_tbody(site_fipe, new_columns_df_bronze)

                    list_fipe_information.append(data)

                # Clean Search
                # We are cleaning because some model are not available for the SPECIFIC Manufacturing Year - Fuel left by the last process.
                # The options were REFRESH the PAGE OR Clean the fields EXCEPT the reference_month.
                # That's why I decided clean the table and add the BRAND again.
                bt_clean = locate_bt(site_fipe, xpath_bt_clean_search)
                click(bt_clean)

                # After Clean, lWe will add again the Brand.
                click(bt_brand)
                add_on(bt_brand, brand)

                # Let's create a CONCURRENCY in this function, while is TRANSFORMING and SAVING I can jump to another code.

                # df = transform_to_df(
                #     spark, list_fipe_information, schema_df_fipe_bronze
                # )
    w = WorkspaceClient()
    # d = w.dbutils.fs.ls('/')
    path_dbfs = "/mnt/json_on_demand/"
    json_formatted = json.dumps(list_fipe_information)
    json_datetime = f"{path_dbfs}data_json_{datetime.now().timestamp()}"
    print(list_fipe_information)
    try:
        w.dbutils.fs.put(json_datetime, json_formatted)
        logger.info("Saved to %s", path_dbfs)
    except AttributeError as e:
        logger.error(e)
    finally:
        close_browser(site_fipe)


if __name__ == "__main__":
    # main()
    print(xpath_bt_brand)
