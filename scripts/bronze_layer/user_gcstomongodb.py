import json
from pymongo import MongoClient
from google.cloud import storage

# === CONFIG ===
GCS_BUCKET = "sp25-thjaya-yelp-data"
GCS_FILE = "yelp_academic_dataset_user.json"
LOCAL_FILE = "/tmp/yelp_academic_dataset_user.json"

MONGO_URI = "mongodb://localhost:27017"
RAW_DB = "yelp"
RAW_COLLECTION = "user"
BRONZE_DB = "bronze_db"
BRONZE_COLLECTION = "user_filtered"

# === STEP 1: GCS → Local VM ===
print("Downloading raw user JSON from GCS...")
client = storage.Client()
bucket = client.bucket(GCS_BUCKET)
blob = bucket.blob(GCS_FILE)
blob.download_to_filename(LOCAL_FILE)
print(f"Downloaded to {LOCAL_FILE}")

# === STEP 2: Load Raw Data → MongoDB (yelp.user) ===
mongo_client = MongoClient(MONGO_URI)
raw_col = mongo_client[RAW_DB][RAW_COLLECTION]

if raw_col.estimated_document_count() > 0:
    print("Raw user collection exists. Dropping...")
    raw_col.drop()

print("Loading raw user data into yelp.user... (batched insert)")
BATCH_SIZE = 1000
batch = []
total = 0

with open(LOCAL_FILE, "r", encoding="utf-8") as f:
    for line in f:
        doc = json.loads(line)
        batch.append(doc)
        if len(batch) >= BATCH_SIZE:
            raw_col.insert_many(batch)
            total += len(batch)
            print(f"Inserted {total} so far...")
            batch.clear()

    if batch:
        raw_col.insert_many(batch)
        total += len(batch)

print(f"Inserted total {total} user records into {RAW_DB}.{RAW_COLLECTION}")

# === STEP 3: Filter users yelping since > 2020 → bronze_db.user_filtered ===
bronze_col = mongo_client[BRONZE_DB][BRONZE_COLLECTION]

if bronze_col.estimated_document_count() > 0:
    print("Bronze user collection exists. Truncating...")
    bronze_col.delete_many({})

query = {"yelping_since": {"$gt": "2020-01-01"}}
projection = {
    "_id": 0,
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
    "compliment_hot": 1,
    "compliment_more": 1,
    "compliment_profile": 1,
    "compliment_cute": 1,
    "compliment_list": 1,
    "compliment_note": 1,
    "compliment_plain": 1,
    "compliment_cool": 1,
    "compliment_funny": 1,
    "compliment_writer": 1,
    "compliment_photos": 1
}

print("Filtering users and writing to bronze... (batched insert)")
cursor = raw_col.find(query, projection)
batch = []
count = 0

for doc in cursor:
    batch.append(doc)
    if len(batch) >= BATCH_SIZE:
        bronze_col.insert_many(batch)
        count += len(batch)
        print(f"Inserted {count} filtered users...")
        batch.clear()

if batch:
    bronze_col.insert_many(batch)
    count += len(batch)

print(f"Filtered and inserted {count} users into {BRONZE_DB}.{BRONZE_COLLECTION}")
