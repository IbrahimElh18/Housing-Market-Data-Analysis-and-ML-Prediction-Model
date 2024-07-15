from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws

# Create a SparkSession
spark = SparkSession.builder.appName("StateZipCode").getOrCreate()

# Read your existing data file
file_path = "/home/cs179g/workspace/output_cleaned/zip_code_market.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Select distinct state and zip_code
state_zip_df = df.select("state", "zip_code").distinct()

# Define the output file path for the CSV
output_file_path = "/home/cs179g/workspace/data/states_and_zip_codes.csv"

# Write the DataFrame to a CSV file
state_zip_df.write.csv(output_file_path, header=True, mode="overwrite")

# Stop the SparkSession
spark.stop()

print("CSV file with states and zip codes created successfully.")
