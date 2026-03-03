import os
import time
import json
import tempfile
import shutil
from io import BytesIO
from datetime import datetime, timezone

from kafka import KafkaConsumer
from minio import Minio

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, trim

# ============================
# CONFIG
# ============================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "public_health_visits")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "public-health-pipeline")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

RAW_BUCKET = os.getenv("RAW_BUCKET", "raw")
CLEANSED_BUCKET = os.getenv("CLEANSED_BUCKET", "cleansed")
CURATED_BUCKET = os.getenv("CURATED_BUCKET", "curated")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
FLUSH_SECONDS = int(os.getenv("FLUSH_SECONDS", "10"))
FLUSH_BY_TIME = os.getenv("FLUSH_BY_TIME", "true").lower() in ("1", "true", "yes")

# Cleansing rules
VALID_CODES = ["B50", "A00", "J10", "A01", "A90"]

# Output naming
PREFIX = os.getenv("FILE_PREFIX", "stream_patient_visits")

# ============================
# HELPERS
# ============================

def ensure_bucket(client: Minio, bucket: str):
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

def spark_find_part_file(folder: str, suffix: str) -> str:
    """
    Spark writes output as a directory; find the part-* file with given suffix.
    """
    for root, _, files in os.walk(folder):
        for f in files:
            if f.startswith("part-") and f.endswith(suffix):
                return os.path.join(root, f)
    return ""

def upload_bytes(minio: Minio, bucket: str, object_name: str, payload: bytes, content_type: str):
    minio.put_object(
        bucket,
        object_name,
        data=BytesIO(payload),
        length=len(payload),
        content_type=content_type
    )

# ============================
# PIPELINE STAGES
# ============================

def stage_raw_upload(minio: Minio, json_lines: list[str]) -> str:
    """
    Upload JSONL batch to raw bucket and return raw object name.
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    raw_object = f"{PREFIX}_{ts}.jsonl"
    payload = ("\n".join(json_lines) + "\n").encode("utf-8")

    upload_bytes(
        minio=minio,
        bucket=RAW_BUCKET,
        object_name=raw_object,
        payload=payload,
        content_type="application/x-ndjson"
    )
    print(f"[RAW] Uploaded {raw_object} ({len(json_lines)} events)")
    return raw_object

def stage_cleanse_to_csv(spark: SparkSession, minio: Minio, raw_object: str, workdir: str) -> str:
    """
    Download raw JSONL, cleanse, write CSV locally, upload to cleansed bucket.
    Returns cleansed CSV object name.
    """
    local_raw = os.path.join(workdir, raw_object)
    minio.fget_object(RAW_BUCKET, raw_object, local_raw)

    df_raw = spark.read.json(local_raw)
    before = df_raw.count()

    df = df_raw

    # Trim common fields
    for c in ["patient_id", "hospital", "county", "disease", "diagnosis_code", "visit_date"]:
        if c in df.columns:
            df = df.withColumn(c, trim(col(c)))

    # Drop missing required fields
    required = [c for c in ["patient_id", "disease", "diagnosis_code", "visit_date"] if c in df.columns]
    df = df.dropna(subset=required)

    # Standardize visit_date
    if "visit_date" in df.columns:
        df = df.withColumn("visit_date", to_date(col("visit_date"), "yyyy-MM-dd")) \
               .filter(col("visit_date").isNotNull())

    # Filter valid codes
    if "diagnosis_code" in df.columns:
        df = df.filter(col("diagnosis_code").isin(VALID_CODES))

    after = df.count()
    removed = before - after
    print(f"[CLEANSE] {raw_object}: before={before}, after={after}, removed={removed}")

    # Select consistent columns if present
    preferred_cols = ["patient_id", "hospital", "county", "visit_date", "disease", "diagnosis_code", "event_time_utc"]
    cols = [c for c in preferred_cols if c in df.columns]
    if cols:
        df = df.select(*cols)

    # Write CSV (single part)
    out_folder = os.path.join(workdir, f"cleansed_{raw_object}".replace(".jsonl", ""))
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(out_folder)

    part_csv = spark_find_part_file(out_folder, ".csv")
    if not part_csv:
        raise RuntimeError(f"Could not find CSV part file for {raw_object} in {out_folder}")

    cleansed_object = f"cleansed_{raw_object}".replace(".jsonl", ".csv")
    minio.fput_object(CLEANSED_BUCKET, cleansed_object, part_csv, content_type="text/csv")
    print(f"[CLEANSED] Uploaded {cleansed_object}")

    return cleansed_object

def stage_csv_to_parquet(spark: SparkSession, minio: Minio, cleansed_object: str, workdir: str) -> str:
    """
    Download cleansed CSV, convert to Parquet, upload to curated bucket.
    Returns curated parquet object name.
    """
    local_csv = os.path.join(workdir, cleansed_object)
    minio.fget_object(CLEANSED_BUCKET, cleansed_object, local_csv)

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(local_csv)

    # Ensure visit_date is date type (helps Trino)
    if "visit_date" in df.columns:
        df = df.withColumn("visit_date", to_date(col("visit_date"), "yyyy-MM-dd"))

    out_folder = os.path.join(workdir, f"parquet_{cleansed_object}".replace(".csv", ""))
    df.coalesce(1).write.mode("overwrite").parquet(out_folder)

    part_parquet = spark_find_part_file(out_folder, ".parquet")
    if not part_parquet:
        raise RuntimeError(f"Could not find Parquet part file for {cleansed_object} in {out_folder}")

    curated_object = cleansed_object.replace("cleansed_", "curated_").replace(".csv", ".parquet")
    minio.fput_object(CURATED_BUCKET, curated_object, part_parquet, content_type="application/octet-stream")
    print(f"[CURATED] Uploaded {curated_object}")

    return curated_object

# ============================
# MAIN LOOP
# ============================

def main():
    # MinIO client
    minio = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    ensure_bucket(minio, RAW_BUCKET)
    ensure_bucket(minio, CLEANSED_BUCKET)
    ensure_bucket(minio, CURATED_BUCKET)

    # Spark (local)
    spark = SparkSession.builder.appName("PublicHealthKafkaMinioPipeline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode("utf-8"),
        consumer_timeout_ms=1000,
    )

    print(f"Kafka: {KAFKA_BOOTSTRAP} topic={KAFKA_TOPIC} group={KAFKA_GROUP_ID}")
    print(f"MinIO: {MINIO_ENDPOINT} raw={RAW_BUCKET} cleansed={CLEANSED_BUCKET} curated={CURATED_BUCKET}")
    print(f"Batch size={BATCH_SIZE} flush_seconds={FLUSH_SECONDS} flush_by_time={FLUSH_BY_TIME}")
    print("Press Ctrl+C to stop.\n")

    buffer = []
    last_flush = time.time()

    workdir = tempfile.mkdtemp(prefix="ph_pipeline_")

    def flush_pipeline():
        nonlocal buffer, last_flush
        if not buffer:
            return

        # (0) Validate JSON lines are at least valid JSON (optional but helps)
        # If some lines are not valid JSON, drop them rather than failing the batch.
        good_lines = []
        bad = 0
        for line in buffer:
            try:
                json.loads(line)
                good_lines.append(line)
            except Exception:
                bad += 1

        if bad:
            print(f"[WARN] Dropped {bad} invalid JSON messages in batch")

        if not good_lines:
            buffer = []
            last_flush = time.time()
            return

        # (1) Upload raw JSONL
        raw_object = stage_raw_upload(minio, good_lines)

        # (2) Cleanse to CSV and upload to cleansed
        cleansed_object = stage_cleanse_to_csv(spark, minio, raw_object, workdir)

        # (3) Convert to Parquet and upload to curated
        _curated_object = stage_csv_to_parquet(spark, minio, cleansed_object, workdir)

        # Reset batch
        buffer = []
        last_flush = time.time()

        # Optional: cleanup local spark output folders to reduce disk usage
        # Keep workdir but clear its contents
        for name in os.listdir(workdir):
            path = os.path.join(workdir, name)
            try:
                if os.path.isdir(path):
                    shutil.rmtree(path, ignore_errors=True)
                else:
                    os.remove(path)
            except Exception:
                pass

    try:
        while True:
            got_any = False

            for msg in consumer:
                got_any = True
                buffer.append(msg.value)

                if len(buffer) >= BATCH_SIZE:
                    flush_pipeline()

            # time-based flush
            now = time.time()
            if FLUSH_BY_TIME and buffer and (now - last_flush) >= FLUSH_SECONDS:
                flush_pipeline()

            if not got_any:
                time.sleep(0.2)

    except KeyboardInterrupt:
        print("\nStopping pipeline...")
    finally:
        # flush remaining
        try:
            flush_pipeline()
        except Exception as e:
            print(f"[ERROR] Final flush failed: {e}")

        consumer.close()
        spark.stop()
        shutil.rmtree(workdir, ignore_errors=True)
        print("Done.")

if __name__ == "__main__":
    main()