import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from minio import Minio

# ============================
# CONFIGURATION
# ============================

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "raw"  # raw zone bucket

NUM_DAYS = 7                 # simulate 7 days of uploads
COUNTIES = ["Nairobi", "Mombasa", "Kisumu", "Nakuru", "Eldoret"]
RECORDS_PER_COUNTY_PER_DAY = 1000  # 1000+ records per county
FILES_PER_DAY = 2            # split each day into multiple CSV files

DISEASES = ["Malaria", "Cholera", "Influenza", "Typhoid", "Dengue"]
DISEASE_PROB = [0.5, 0.05, 0.3, 0.1, 0.05]  # skew for outbreak scenario

LOCAL_TMP_DIR = "tmp_csv"
os.makedirs(LOCAL_TMP_DIR, exist_ok=True)

# ============================
# CONNECT TO MINIO
# ============================

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Create bucket if it doesn't exist
if not client.bucket_exists(MINIO_BUCKET):
    client.make_bucket(MINIO_BUCKET)

# ============================
# GENERATE AND UPLOAD DATA
# ============================

today = datetime.today()

for day_offset in range(NUM_DAYS):
    day = today - timedelta(days=NUM_DAYS - day_offset)
    
    for file_idx in range(FILES_PER_DAY):
        records = []

        for county in COUNTIES:
            for rec_id in range(RECORDS_PER_COUNTY_PER_DAY // FILES_PER_DAY):
                patient_id = f"{county[:3].upper()}{day_offset}{file_idx}{rec_id:04d}"
                hospital = np.random.choice([
                    f"{county} General Hospital",
                    f"{county} Medical Center",
                    f"{county} Clinic"
                ])
                visit_date = day.strftime("%Y-%m-%d")
                disease = np.random.choice(DISEASES, p=DISEASE_PROB)
                diagnosis_code = {
                    "Malaria": "B50",
                    "Cholera": "A00",
                    "Influenza": "J10",
                    "Typhoid": "A01",
                    "Dengue": "A90"
                }[disease]

                records.append({
                    "patient_id": patient_id,
                    "hospital": hospital,
                    "county": county,
                    "visit_date": visit_date,
                    "disease": disease,
                    "diagnosis_code": diagnosis_code
                })
        
        df = pd.DataFrame(records)
        filename = f"patient_data_day{day_offset+1}_{file_idx+1}.csv"
        local_path = os.path.join(LOCAL_TMP_DIR, filename)
        df.to_csv(local_path, index=False)
        
        # ============================
        # UPLOAD TO MINIO
        # ============================
        client.fput_object(MINIO_BUCKET, filename, local_path)
        print(f"Uploaded {filename} ({len(records)} records) to bucket '{MINIO_BUCKET}'")

print("All synthetic CSV data files generated and uploaded to MinIO!")
