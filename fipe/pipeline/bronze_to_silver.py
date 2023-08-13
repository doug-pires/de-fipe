import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from fipe.conf.read_configuration import bronze_path, silver_path, table_name_bronze
from fipe.elt.load import read_delta_table, save_delta_table
from fipe.elt.transform import add_columns, parse_month_year
from fipe.scripts.get_spark import SparkSessionManager

# Register UDF
transform_to_first_date = udf(parse_month_year, StringType())


def main():
    spark_manager = SparkSessionManager(__name__)
    spark = spark_manager.get_spark_session()
    df_fipe_bronze = read_delta_table(spark, bronze_path, table_name_bronze)

    cols_to_enrich_silver = {
        "reference_start_date": transform_to_first_date(F.col("reference_month")).cast(
            "date"
        ),
        "reference_year": F.year("reference_start_date"),
        "manufacturing_year": F.trim(
            F.split("manufacturing_year_fuel", " ").getItem(0)
        ),
        "fuel_type": F.trim(F.split("manufacturing_year_fuel", " ").getItem(1)),
    }

    df_silver = add_columns(df_fipe_bronze, cols_to_enrich_silver)
    print(df_silver.show())


if __name__ == "__main__":
    main()
