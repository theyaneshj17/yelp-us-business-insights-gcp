from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# === CONFIG ===
MONGO_URI = "mongodb://10.128.0.3:27017"
MONGO_DB = "bronze_db"
REVIEW_COLLECTION = "review_filtered"
BQ_OUTPUT_TABLE = "sp25-i535-thjaya-yelpanalysis.silver_layer.review_filtered"
GCS_TEMP_BUCKET = "sp25-thjaya-yelp-data"

# === Spark session with optimized configs ===
spark = SparkSession.builder \
    .appName("Yelp Review Silver Layer to BigQuery") \
    .config("spark.jars.packages", ",".join([
        "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1"
    ])) \
    .config("spark.mongodb.input.partitioner", "MongoPaginateBySizePartitioner") \
    .config("spark.mongodb.input.partitionerOptions.partitionSizeMB", "64") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.default.parallelism", "8") \
    .config("spark.mongodb.read.batchSize", "4096") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# === Read from MongoDB with projection at source ===
review_df = spark.read.format("mongodb") \
    .option("spark.mongodb.read.connection.uri", f"{MONGO_URI}/{MONGO_DB}.{REVIEW_COLLECTION}") \
    .option("spark.mongodb.read.readPreference.name", "secondaryPreferred") \
    .option("pipeline", """[
        {"$project": {
            "review_id": 1, 
            "user_id": 1, 
            "business_id": 1, 
            "stars": 1, 
            "date": 1,
            "_id": 0
        }}
    ]""") \
    .load()

# === Cache the dataframe if using it multiple times ===
# review_df.cache()  # Uncomment if you'll reuse this DataFrame

# === Write to BigQuery with optimized options ===
review_df.write \
    .format("bigquery") \
    .option("table", BQ_OUTPUT_TABLE) \
    .option("temporaryGcsBucket", GCS_TEMP_BUCKET) \
    .option("writeMethod", "direct") \
    .option("createDisposition", "CREATE_IF_NEEDED") \
    .option("partitionField", "date") \
    .mode("overwrite") \
    .save()

# Optional: Stop Spark session when done
spark.stop()