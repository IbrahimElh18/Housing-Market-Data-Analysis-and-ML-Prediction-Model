from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DateType, FloatType


def prepare_data(df: DataFrame, data_name: str) -> DataFrame:
    df = df.withColumn("period_begin", col("period_begin").cast(DateType()))
    df = df.withColumn("period_end", col("period_end").cast(DateType()))
    df = df.withColumn("median_sale_price", col("median_sale_price").cast(FloatType()))
    df = df.withColumn("median_list_price", col("median_list_price").cast(FloatType()))

    fill_values = {
        'period_duration': '0',
        'median_sale_price': 0.0,
        'median_sale_price_mom': 0.0,
        'median_sale_price_yoy': 0.0,
        'median_list_price': 0.0,
        'median_list_price_mom': 0.0,
        'median_list_price_yoy': 0.0,
        'median_ppsf': 0.0,
        'median_ppsf_mom': 0.0,
        'median_ppsf_yoy': 0.0,
        'median_list_ppsf': 0.0,
        'median_list_ppsf_mom': 0.0,
        'median_list_ppsf_yoy': 0.0,
        'homes_sold': 0.0,
        'homes_sold_mom': 0.0,
        'homes_sold_yoy': 0.0,
        'pending_sales': 0.0,
        'pending_sales_mom': 0.0,
        'pending_sales_yoy': 0.0,
        'new_listings': 0.0,
        'new_listings_mom': 0.0,
        'new_listings_yoy': 0.0,
        'inventory': 0.0,
        'inventory_mom': 0.0,
        'inventory_yoy': 0.0,
        'months_of_supply': 0.0,
        'months_of_supply_mom': 0.0,
        'months_of_supply_yoy': 0.0,
        'median_dom': 0.0,
        'median_dom_mom': 0.0,
        'median_dom_yoy': 0.0,
        'avg_sale_to_list': 0.0,
        'avg_sale_to_list_mom': 0.0,
        'avg_sale_to_list_yoy': 0.0,
        'sold_above_list': 0.0,
        'sold_above_list_mom': 0.0,
        'sold_above_list_yoy': 0.0,
        'price_drops': 0.0,
        'price_drops_mom': 0.0,
        'price_drops_yoy': 0.0,
        'off_market_in_two_weeks': 0.0,
        'off_market_in_two_weeks_mom': 0.0,
        'off_market_in_two_weeks_yoy': 0.0
    }

    
    df = df.fillna(fill_values)

    print(f"Prepared data for {data_name}")
    return df


state_df = spark.read.csv("hdfs://localhost:9000/user/cs179g/data/state_market_tracker.tsv000", sep='\t', header=True)
state_df = prepare_data(state_df, "state")

us_df = spark.read.csv("hdfs://localhost:9000/user/cs179g/data/us_national_market_tracker.tsv000", sep='\t', header=True)
us_df = prepare_data(us_df, "us")

zip_df = spark.read.csv("hdfs://localhost:9000/user/cs179g/data/zip_code_market_tracker.tsv000", sep='\t', header=True)
zip_df = prepare_data(zip_df, "zip")

neighborhood_df = spark.read.csv("hdfs://localhost:9000/user/cs179g/data/neighborhood_market_tracker.tsv000", sep='\t', header=True)
neighborhood_df = prepare_data(neighborhood_df, "neighborhood")

county_df = spark.read.csv("hdfs://localhost:9000/user/cs179g/data/county_market_tracker.tsv000", sep='\t', header=True)
county_df = prepare_data(county_df, "county")

state_df.write.csv("hdfs://localhost:9000/user/cs179g/output/state_market", mode="overwrite", header=True)
us_df.write.csv("hdfs://localhost:9000/user/cs179g/output/us_national_market", mode="overwrite", header=True)
zip_df.write.csv("hdfs://localhost:9000/user/cs179g/output/zip_code_market", mode="overwrite", header=True)
neighborhood_df.write.csv("hdfs://localhost:9000/user/cs179g/output/neighborhood_market", mode="overwrite", header=True)
county_df.write.csv("hdfs://localhost:9000/user/cs179g/output/county_market", mode="overwrite", header=True)
