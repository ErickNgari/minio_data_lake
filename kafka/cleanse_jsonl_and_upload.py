from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, trim
from minio import Minio
import tempfile
import os
import shutil

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

# Temp directories
TMP_DIR = tempfile.mkdtemp(prefix="raw_jsonl_")
OUT_DIR = tempfile.mkdtemp(prefix="cleansed_csv_")

# ============================
# CONNECT TO MINIO
# ============================

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

if not client.bucket_exists(CLEANSED_BUCKET):
    client.make_bucket(CLEANSED_BUCKET)

# ============================
# INITIALIZE SPARK
# ============================

spark = SparkSession.builder \
    .appName("PublicHealthCleanseJSONLToCSV") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


def find_single_part_file(folder: str, suffix: str):
    """Spark writes to a folder; find the part-* file with a given suffix (e.g. .csv)."""
    for root, _, files in os.walk(folder):
        for f in files:
            if f.startswith("part-") and f.endswith(suffix):
                return os.path.join(root, f)
    return None


try:
    # ============================
    # DOWNLOAD RAW JSONL FILES
    # ============================
    raw_objects = list(client.list_objects(RAW_BUCKET, recursive=True))
    jsonl_objects = [o for o in raw_objects if o.object_name.lower().endswith(".jsonl")]

    if not jsonl_objects:
        raise RuntimeError(f"No .jsonl files found in bucket '{RAW_BUCKET}'")

    local_files = []
    for obj in jsonl_objects:
        local_path = os.path.join(TMP_DIR, os.path.basename(obj.object_name))
        client.fget_object(RAW_BUCKET, obj.object_name, local_path)
        local_files.append(local_path)
        print(f"Downloaded {obj.object_name}")

    # ============================
    # CLEANSE AND UPLOAD (AS CSV)
    # ============================
    for file_path in local_files:
        in_name = os.path.basename(file_path)

        # Read JSON Lines (JSONL)
        df_raw = spark.read.json(file_path)

        total_before = df_raw.count()

        # ---- cleansing rules ----
        df = df_raw

        # Trim common fields (only if column exists)
        for c in ["patient_id", "hospital", "county", "disease", "diagnosis_code", "visit_date"]:
            if c in df.columns:
                df = df.withColumn(c, trim(col(c)))

        # Drop rows missing required fields
        required = [c for c in ["patient_id", "disease", "diagnosis_code", "visit_date"] if c in df.columns]
        df = df.dropna(subset=required)

        # Standardize visit_date (expects yyyy-MM-dd)
        if "visit_date" in df.columns:
            df = df.withColumn("visit_date", to_date(col("visit_date"), "yyyy-MM-dd")) \
                   .filter(col("visit_date").isNotNull())

        # Filter valid diagnosis codes
        if "diagnosis_code" in df.columns:
            df = df.filter(col("diagnosis_code").isin(VALID_CODES))

        total_after = df.count()
        removed = total_before - total_after
        print(f"[{in_name}] before={total_before}, after={total_after}, removed={removed}")

        # Choose consistent output columns (only those that exist)
        preferred_cols = ["patient_id", "hospital", "county", "visit_date", "disease", "diagnosis_code", "event_time_utc"]
        cols = [c for c in preferred_cols if c in df.columns]
        if cols:
            df = df.select(*cols)

        # Write cleansed CSV locally (Spark writes a folder)
        base = in_name.replace(".jsonl", "")
        out_folder = os.path.join(OUT_DIR, f"cleansed_{base}_csv_out")
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(out_folder)

        part_csv = find_single_part_file(out_folder, ".csv")
        if not part_csv:
            raise RuntimeError(f"[{in_name}] Could not find Spark part-*.csv output in {out_folder}")

        # Upload to MinIO cleansed bucket (root)
        out_object_name = f"cleansed_{base}.csv"
        client.fput_object(
            CLEANSED_BUCKET,
            out_object_name,
            part_csv,
            content_type="text/csv"
        )
        print(f"Uploaded cleansed CSV {out_object_name} to bucket '{CLEANSED_BUCKET}'")

    print("✅ JSONL cleansing completed and uploaded as CSV to MinIO cleansed bucket!")

finally:
    spark.stop()
    shutil.rmtree(TMP_DIR, ignore_errors=True)
    shutil.rmtree(OUT_DIR, ignore_errors=True)