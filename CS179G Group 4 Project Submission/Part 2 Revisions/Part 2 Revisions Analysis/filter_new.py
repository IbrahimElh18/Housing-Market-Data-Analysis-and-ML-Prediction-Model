#preprocessing dataset for machine learning
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import avg
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("preprocess_data").getOrCreate()
file_path = "/home/cs179g/workspace/ml_analysis/filtered_data.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)
df.printSchema()
