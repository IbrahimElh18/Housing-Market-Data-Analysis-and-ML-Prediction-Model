from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Create SparkSession
spark = SparkSession.builder.appName("housing_market_trajectory").getOrCreate()

# Load data
file_path = "/home/cs179g/workspace/output_cleaned/zip_code_market.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Repartition DataFrame to 2 partitions
df = df.repartition(2)

# Preprocessing: Select required columns
housing_trajectory_columns = ['median_sale_price', 'median_list_price', 'median_ppsf', 'period_begin', 'homes_sold', 'period_end', 'region_type', 'region', 'state', 'state_code', 'property_type', 'parent_metro_region', 'last_updated']
housing_trajectory_df = df.select(housing_trajectory_columns)

# Cache the DataFrame for reuse
housing_trajectory_df.cache()

# Save the DataFrame as CSV (optional)
housing_trajectory_df.write.csv("/home/cs179g/workspace/ml_analysis/housing_trajectory.csv", header=True, mode="overwrite")

# Analysis: Average sale price by state
avg_sale_price_by_state = housing_trajectory_df.groupBy("state").agg(avg("median_sale_price").alias("avg_median_sale_price")).orderBy("state")

# Write results to CSV
avg_sale_price_by_state.write.csv("/home/cs179g/workspace/data/average_sale_price_by_state.csv", header=True, mode="overwrite")

# Analysis: Average sale price by region
avg_sale_price_by_region = housing_trajectory_df.groupBy("region").agg(avg("median_sale_price").alias("avg_median_sale_price")).orderBy("region")
avg_sale_price_by_region.write.csv("/home/cs179g/workspace/data/average_sale_price_by_region.csv", header=True, mode="overwrite")

# Analysis: Average list price by state
avg_list_price_by_state = housing_trajectory_df.groupBy("state").agg(avg("median_list_price").alias("avg_median_list_price")).orderBy("state")
avg_list_price_by_state.write.csv("/home/cs179g/workspace/data/average_list_price_by_state.csv", header=True, mode="overwrite")

# Analysis: Average list price by region
avg_list_price_by_region = housing_trajectory_df.groupBy("region").agg(avg("median_list_price").alias("avg_median_list_price")).orderBy("region")
avg_list_price_by_region.write.csv("/home/cs179g/workspace/data/average_list_price_by_region.csv", header=True, mode="overwrite")

# Analysis: Median price per square foot by state
median_ppsf_by_state = housing_trajectory_df.groupBy("state").agg(avg("median_ppsf").alias("avg_median_ppsf")).orderBy("state")
median_ppsf_by_state.write.csv("/home/cs179g/workspace/data/median_ppsf_by_state.csv", header=True, mode="overwrite")

# Analysis: Median price per square foot by region
median_ppsf_by_region = housing_trajectory_df.groupBy("region").agg(avg("median_ppsf").alias("avg_median_ppsf")).orderBy("region")
median_ppsf_by_region.write.csv("/home/cs179g/workspace/data/median_ppsf_by_region.csv", header=True, mode="overwrite")

# Analysis: Average price per state by property type
average_df = housing_trajectory_df.groupBy("state", "property_type").agg(avg("median_sale_price").alias("avg_median_sale_price"), avg("median_list_price").alias("avg_median_list_price"), avg("median_ppsf").alias("avg_median_ppsf"))
average_df.write.csv("/home/cs179g/workspace/data/averages_by_prop_type_state.csv", header=True, mode="overwrite")

# Stop SparkSession
spark.stop()
