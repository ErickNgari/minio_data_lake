import os
import time
from io import BytesIO
from datetime import datetime, timezone
from kafka import KafkaConsumer
from minio import Minio

# ============================
# CONFIG
# ============================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "public_health_visits")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "minio-sink")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "raw")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
FLUSH_SECONDS = int(os.getenv("FLUSH_SECONDS", "10"))

# If true, create one JSONL per flush; if false, only flush by batch size
FLUSH_BY_TIME = os.getenv("FLUSH_BY_TIME", "true").lower() in ("1", "true", "yes")

def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode("utf-8"),
        consumer_timeout_ms=1000,  # allows periodic time-based flush
    )

    minio = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    if not minio.bucket_exists(MINIO_BUCKET):
        minio.make_bucket(MINIO_BUCKET)

    buffer = []
    last_flush = time.time()

    def flush():
        nonlocal buffer, last_flush
        if not buffer:
            return

        payload = ("\n".join(buffer) + "\n").encode("utf-8")
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        object_name = f"stream_patient_visits_{ts}.jsonl"

        minio.put_object(
            MINIO_BUCKET,
            object_name,
            data=BytesIO(payload),
            length=len(payload),
            content_type="application/x-ndjson",
        )

        print(f"Uploaded {object_name} to bucket '{MINIO_BUCKET}' ({len(buffer)} events)")
        buffer = []
        last_flush = time.time()

    print(f"Consuming Kafka topic='{KAFKA_TOPIC}' bootstrap='{KAFKA_BOOTSTRAP}' group='{KAFKA_GROUP_ID}'")
    print(f"Writing JSONL batches to MinIO '{MINIO_ENDPOINT}/{MINIO_BUCKET}'")
    print(f"Batch size: {BATCH_SIZE} | Flush seconds: {FLUSH_SECONDS} | Time flush enabled: {FLUSH_BY_TIME}")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            got_any = False
            for msg in consumer:
                got_any = True
                buffer.append(msg.value)

                if len(buffer) >= BATCH_SIZE:
                    flush()

            # If no messages arrived in this poll window, consider time-based flush
            now = time.time()
            if FLUSH_BY_TIME and buffer and (now - last_flush) >= FLUSH_SECONDS:
                flush()

            # tiny sleep to avoid tight loop
            if not got_any:
                time.sleep(0.2)

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        # Flush remaining
        if buffer:
            flush()
        consumer.close()
        print("Done.")

if __name__ == "__main__":
    main()