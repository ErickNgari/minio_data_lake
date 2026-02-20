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
RAW_BUCKET = "raw"
CLEANSED_BUCKET = "cleansed"

# Temporary local folder for downloading files
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

# Create cleansed bucket if it doesn't exist
if not client.bucket_exists(CLEANSED_BUCKET):
    client.make_bucket(CLEANSED_BUCKET)

# ============================
# INITIALIZE SPARK
# ============================

spark = SparkSession.builder \
    .appName("PublicHealthDataCleanseCSV") \
    .getOrCreate()

# ============================
# DOWNLOAD RAW CSV FILES FROM MINIO
# ============================

raw_files = client.list_objects(RAW_BUCKET)
csv_files_local = []

for obj in raw_files:
    local_path = os.path.join(TMP_DIR, obj.object_name)
    client.fget_object(RAW_BUCKET, obj.object_name, local_path)
    csv_files_local.append(local_path)
    print(f"Downloaded {obj.object_name} for cleansing")

# ============================
# READ, CLEANSE, AND UPLOAD
# ============================

for file_path in csv_files_local:
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
    
    # --------------------
    # Cleansing rules
    # --------------------
    # 1. Remove rows with missing patient_id or disease
    df = df.dropna(subset=["patient_id", "disease"])
    
    # 2. Standardize visit_date
    df = df.withColumn("visit_date", to_date(col("visit_date"), "yyyy-MM-dd"))
    
    # 3. Validate disease codes
    valid_codes = ["B50", "A00", "J10", "A01", "A90"]
    df = df.filter(col("diagnosis_code").isin(valid_codes))
    
    # Save cleansed CSV locally
    filename = os.path.basename(file_path)
    cleansed_path_local = os.path.join(TMP_DIR, f"cleansed_{filename}")
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(cleansed_path_local)
    
    # Upload cleansed CSV(s) to MinIO
    # Spark writes CSV as folder with part-*.csv, so find the actual CSV file
    for root, dirs, files in os.walk(cleansed_path_local):
        for f in files:
            if f.endswith(".csv"):
                file_path_to_upload = os.path.join(root, f)
                object_name = filename # f"patient_data_cleansed/{filename}"
                client.fput_object(CLEANSED_BUCKET, filename, file_path_to_upload)
                print(f"Uploaded cleansed {object_name} to cleansed bucket")

print("Data cleansing completed and uploaded as CSV to MinIO cleansed bucket!")
spark.stop()
