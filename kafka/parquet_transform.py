from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from minio import Minio
import tempfile
import os

# ============================
# CONFIGURATION
# ============================

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
CLEANSED_BUCKET = "cleansed"
CURATED_BUCKET = "curated"

# Temporary folder for local downloads
TMP_DIR = tempfile.mkdtemp()

# ============================
# CONNECT TO MINIO
# ============================

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Create curated bucket if it doesn't exist
if not client.bucket_exists(CURATED_BUCKET):
    client.make_bucket(CURATED_BUCKET)

# ============================
# INITIALIZE SPARK
# ============================

spark = SparkSession.builder \
    .appName("PublicHealthDataCurate") \
    .getOrCreate()

# ============================
# DOWNLOAD CLEANSSED CSV FILES
# ============================

cleansed_files = client.list_objects(CLEANSED_BUCKET)
csv_files_local = []

for obj in cleansed_files:
    local_path = os.path.join(TMP_DIR, obj.object_name)
    client.fget_object(CLEANSED_BUCKET, obj.object_name, local_path)
    csv_files_local.append(local_path)
    print(f"Downloaded {obj.object_name} for Parquet conversion")

# ============================
# READ, CONVERT TO PARQUET, AND UPLOAD
# ============================

for file_path in csv_files_local:
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
    
    # Ensure visit_date is proper date type
    df = df.withColumn("visit_date", to_date(col("visit_date"), "yyyy-MM-dd"))
    
    # Save locally as Parquet
    filename = os.path.basename(file_path).replace(".csv", ".parquet")
    parquet_local_path = os.path.join(TMP_DIR, filename)
    df.coalesce(1).write.mode("overwrite").parquet(parquet_local_path)
    
    # Spark writes Parquet as folder with part-*.parquet, find the actual parquet file
    for root, dirs, files in os.walk(parquet_local_path):
        for f in files:
            if f.endswith(".parquet"):
                file_path_to_upload = os.path.join(root, f)
                object_name = filename  # store in root of curated bucket
                client.fput_object(CURATED_BUCKET, object_name, file_path_to_upload)
                print(f"Uploaded {object_name} to curated bucket")

print("All CSV files converted to Parquet and uploaded to curated bucket!")
spark.stop()
