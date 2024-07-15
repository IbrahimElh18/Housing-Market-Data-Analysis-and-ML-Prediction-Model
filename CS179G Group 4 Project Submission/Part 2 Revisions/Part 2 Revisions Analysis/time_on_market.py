from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import avg
from pyspark.sql import functions as F
from pyspark.sql.functions import col, year, month

spark = SparkSession.builder.appName("time_on_market_analysis").getOrCreate()
file_path = "/home/cs179g/workspace/output_cleaned/zip_code_market.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)
df.printSchema()

#showing the median days on market for properties by year
monthly_data = df.withColumn("year_month", F.concat(year("period_begin"), F.lit("-"), month("period_begin")))
monthly_stats = monthly_data.groupBy("year_month").agg(avg("price_drops").alias("avg_price_drops"),median("median_dom").alias("median_dom")).orderBy("year_month")
monthly_stats = monthly_stats.filter(col("avg_price_drops").isNull() | (col("avg_price_drops") == 0))
monthly_stats = monthly_stats.drop("avg_price_drops")
monthly_stats = monthly_stats.dropna(subset=["year_month", "median_dom"])
monthly_stats.show()
monthly_stats.write.csv("/home/cs179g/workspace/data/monthly_stats", header = True, mode = "overwrite")

#shows the median days on market of each property type by state
state_property_stats = df.groupBy("state", "property_type").agg(median("median_dom").alias("median_dom")).orderBy("state", "property_type")
state_property_stats = state_property_stats.dropna(subset=["state", "property_type", "median_dom"])
state_property_stats.show()
state_property_stats.write.csv("/home/cs179g/workspace/data/median_dom_by_property_type_state.csv", header = True, mode = "overwrite")

spark.stop()
