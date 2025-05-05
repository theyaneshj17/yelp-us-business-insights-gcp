from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, upper, col

# === CONFIG ===
MONGO_URI = "mongodb://10.128.0.3:27017"  # Adjust if needed
MONGO_DB = "bronze_db"
COLLECTION = "state_mapping"
BQ_OUTPUT_TABLE = "sp25-i535-thjaya-yelpanalysis.silver_layer.state_mapping"

# === Start Spark session ===
spark = SparkSession.builder \
    .appName("Silver Layer - State Mapping to BigQuery") \
    .config("spark.jars.packages", ",".join([
        "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1"
    ])) \
    .getOrCreate()

# === Read from MongoDB ===
state_df = spark.read.format("mongodb") \
    .option("spark.mongodb.read.connection.uri", f"{MONGO_URI}/{MONGO_DB}.{COLLECTION}") \
    .load()

# Optional: Show schema and preview (for debugging)
# state_df.printSchema()
# state_df.show(5)

# === Clean and select relevant fields ===
state_df_clean = state_df.select(
    trim(upper(col("Abbreviation"))).alias("state"),
    trim(col("Name")).alias("state_name"),
    trim(col("Region")).alias("region")
).filter(col("state").isNotNull())

# === Write to BigQuery ===
state_df_clean.write \
    .format("bigquery") \
    .option("table", BQ_OUTPUT_TABLE) \
    .option("writeMethod", "direct") \
    .mode("overwrite") \
    .save()

# === Stop Spark session ===
spark.stop()
