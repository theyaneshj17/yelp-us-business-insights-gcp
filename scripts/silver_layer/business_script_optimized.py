from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, split

# === CONFIG ===
MONGO_URI = "mongodb://10.128.0.3:27017"
MONGO_DB = "bronze_db"
BUSINESS_COLLECTION = "business_filtered"
BQ_OUTPUT_TABLE = "sp25-i535-thjaya-yelpanalysis.silver_layer.business_data"
GCS_TEMP_BUCKET = "sp25-thjaya-yelp-data"  # Add your temp bucket

# === Spark session with optimized configs ===
spark = SparkSession.builder \
    .appName("Yelp Business Silver Layer") \
    .config("spark.jars.packages", ",".join([
        "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1"
    ])) \
    .config("spark.mongodb.input.partitioner", "MongoPaginateBySizePartitioner") \
    .config("spark.mongodb.input.partitionerOptions.partitionSizeMB", "64") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.mongodb.read.batchSize", "4096") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()



# === Read business collection with projection push-down ===
business_df = spark.read.format("mongodb") \
    .option("spark.mongodb.read.connection.uri", f"{MONGO_URI}/{MONGO_DB}.{BUSINESS_COLLECTION}") \
    .option("spark.mongodb.read.readPreference.name", "secondaryPreferred") \
    .option("pipeline", """[
        {"$project": {
            "business_id": 1,
            "state": 1,
            "name": 1,
            "categories": 1,
            "city": 1,
            "stars": 1,
            "review_count": 1,
            "_id": 0
        }},
        {"$match": {
            "review_count": {"$ne": null}
        }}
    ]""") \
    .load()

# === Clean business fields ===
business_df_clean = business_df.select(
    col("business_id"),
    trim(upper(col("state"))).alias("state"),
    col("name"),
    col("categories"),
    col("city"),
    col("stars"),
    col("review_count")
)


df_filtered = business_df_clean.withColumn("categories", split(col("categories"), ",\\s*"))

# === Write to BigQuery ===
df_filtered.write \
    .format("bigquery") \
    .option("table", BQ_OUTPUT_TABLE) \
    .option("temporaryGcsBucket", GCS_TEMP_BUCKET) \
    .option("writeMethod", "direct") \
    .option("createDisposition", "CREATE_IF_NEEDED") \
    .mode("overwrite") \
    .save()

spark.stop()