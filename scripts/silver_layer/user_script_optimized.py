from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# === CONFIG ===
MONGO_URI = "mongodb://10.128.0.3:27017"
MONGO_DB = "bronze_db"
USER_COLLECTION = "user_filtered"
BQ_OUTPUT_TABLE = "sp25-i535-thjaya-yelpanalysis.silver_layer.Users"
GCS_TEMP_BUCKET = "sp25-thjaya-yelp-data"  # Add your temp bucket

# === Spark session with optimized configs ===
spark = SparkSession.builder \
    .appName("Yelp User Silver Layer to BigQuery") \
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

# === Read filtered user data from MongoDB with projection push-down ===
user_df = spark.read.format("mongodb") \
    .option("spark.mongodb.read.connection.uri", f"{MONGO_URI}/{MONGO_DB}.{USER_COLLECTION}") \
    .option("spark.mongodb.read.readPreference.name", "secondaryPreferred") \
    .option("pipeline", """[
        {"$match": {"review_count": {"$ne": null}}},
        {"$project": {
            "user_id": 1, 
            "name": 1, 
            "review_count": 1, 
            "yelping_since": 1,
            "friends": 1, 
            "useful": 1, 
            "funny": 1, 
            "cool": 1, 
            "fans": 1, 
            "elite": 1, 
            "average_stars": 1,
            "_id": 0
        }}
    ]""") \
    .load()

# === Write to BigQuery ===
user_df.write \
    .format("bigquery") \
    .option("table", BQ_OUTPUT_TABLE) \
    .option("temporaryGcsBucket", GCS_TEMP_BUCKET) \
    .option("writeMethod", "direct") \
    .option("createDisposition", "CREATE_IF_NEEDED") \
    .mode("overwrite") \
    .save()

spark.stop()