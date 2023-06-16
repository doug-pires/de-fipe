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
from fipe.elt.extract.utils import scrape_options_models, scrape_manufacturing_year_fuel
from fipe.elt.load.utils import (
    transform_df_to_list,
    save_delta_table_partitioned,
    read_delta_table,
)
from fipe.elt.transform.utils import transform_list_to_df, add_column
from fipe.scripts.get_spark import SparkSessionManager
import fipe.pipeline.read_configuration as cf

import time

spark_manager = SparkSessionManager(app_name=__name__)
spark = spark_manager.get_spark_session()


def main():
    from dev.dev_utils import path_dev

    df_month_year_as_delta = read_delta_table(spark, path_dev, "reference_month")
    df_brands = read_delta_table(spark, path_dev, "brands")
    list_reference_month_year = transform_df_to_list(df_month_year_as_delta)
    list_brands = transform_df_to_list(df_brands)
    print(list_brands[:3])
    print(type(list_brands[:3]))
    site_fipe = open_chrome(cf.url)
    scroll_to_element(site_fipe, cf.xpath_search_car)
    bt = locate_bt(site_fipe, cf.xpath_search_car)
    click(bt)
    for month_year in list_reference_month_year[:1]:
        time.sleep(0.5)
        bt_month_year = locate_bt(
            driver=site_fipe,
            xpath=cf.xpath_bt_month_year,
        )
        click(bt_month_year)
        time.sleep(0.5)
        add_on(bt_or_box=bt_month_year, info=month_year)
        for brand in list_brands[:2]:
            time.sleep(1)
            bt_brand = locate_bt(site_fipe, cf.xpath_bt_brand)
            add_on(bt_brand, brand)
            list_models = scrape_options_models(site_fipe)
            # df_models = transform_list_to_df(spark, list_models, cf.schema_models)
            # df_with_brand = add_column(df_models, "brand", brand[0])
            # save_delta_table_partitioned(
            #     df=df_with_brand,
            #     path=path_dev,
            #     delta_table_name="models",
            #     partition_by="brand",
            # )
            for model in list_models[:2]:
                bt_model = locate_bt(driver=site_fipe, xpath=cf.xpath_bt_model)
                click(bt_model)
                add_on(bt_model, model)
                list_manufacturing_year_fuel = scrape_manufacturing_year_fuel(site_fipe)
                for manufacturing_year in list_manufacturing_year_fuel[:2]:
                    print(
                        f"Brand: {brand} :-: Model: {model} :-: Manufacturing Year: {manufacturing_year}"
                    )

        # save_delta_table_partitioned()

    close_browser(site_fipe)


if __name__ == "__main__":
    main()
