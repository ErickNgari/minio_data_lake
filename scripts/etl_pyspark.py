from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, trim, lower
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

# Valid codes expected after cleanup
VALID_CODES = ["B50", "A00", "J10", "A01", "A90"]

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

spark.sparkContext.setLogLevel("WARN")

# ============================
# DOWNLOAD RAW CSV FILES FROM MINIO
# ============================

raw_files = list(client.list_objects(RAW_BUCKET))
if not raw_files:
    raise RuntimeError(f"No files found in bucket '{RAW_BUCKET}'")

csv_files_local = []

for obj in raw_files:
    # Optional: only process CSVs
    if not obj.object_name.lower().endswith(".csv"):
        continue

    local_path = os.path.join(TMP_DIR, obj.object_name)
    client.fget_object(RAW_BUCKET, obj.object_name, local_path)
    csv_files_local.append(local_path)
    print(f"Downloaded {obj.object_name} for cleansing")

if not csv_files_local:
    raise RuntimeError(f"No CSV files found in bucket '{RAW_BUCKET}'")

# ============================
# READ, CLEANSE, AND UPLOAD
# ============================

for file_path in csv_files_local:
    filename = os.path.basename(file_path)

    df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)

    total_before = df_raw.count()

    # --------------------
    # Cleansing rules
    # --------------------
    df = df_raw

    # 1) Normalize key fields (remove extra spaces)
    df = df.withColumn("patient_id", trim(col("patient_id"))) \
           .withColumn("county", trim(col("county"))) \
           .withColumn("hospital", trim(col("hospital"))) \
           .withColumn("disease", trim(col("disease"))) \
           .withColumn("diagnosis_code", trim(col("diagnosis_code")))

    # 2) Remove rows with missing patient_id or disease or diagnosis_code
    df = df.dropna(subset=["patient_id", "disease", "diagnosis_code", "visit_date"])

    # 3) Standardize visit_date
    df = df.withColumn("visit_date", to_date(col("visit_date"), "yyyy-MM-dd")) \
           .filter(col("visit_date").isNotNull())

    # 4) Validate disease codes (filters out your 3 extra invalid diseases/codes)
    df = df.filter(col("diagnosis_code").isin(VALID_CODES))

    total_after = df.count()
    removed = total_before - total_after

    print(f"[{filename}] Records before: {total_before}, after: {total_after}, removed: {removed}")

    # --------------------
    # Write cleansed CSV locally (Spark outputs folder)
    # --------------------
    cleansed_path_local = os.path.join(TMP_DIR, f"cleansed_{filename}")
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(cleansed_path_local)

    # Find the actual part-*.csv file produced by Spark
    part_csv = None
    for root, dirs, files in os.walk(cleansed_path_local):
        for f in files:
            if f.endswith(".csv"):
                part_csv = os.path.join(root, f)
                break
        if part_csv:
            break

    if not part_csv:
        raise RuntimeError(f"[{filename}] No CSV part file found after Spark write.")

    # Upload cleansed file to MinIO in bucket root (your requirement)
    object_name = filename
    client.fput_object(CLEANSED_BUCKET, object_name, part_csv)
    print(f"Uploaded cleansed {object_name} to bucket '{CLEANSED_BUCKET}'")

print("✅ Data cleansing completed and uploaded as CSV to MinIO cleansed bucket!")
spark.stop()
