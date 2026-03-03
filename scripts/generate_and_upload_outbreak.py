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

NUM_DAYS = 30  # 1 month
COUNTIES = ["Nairobi", "Mombasa", "Kisumu", "Nakuru", "Eldoret"]
RECORDS_PER_COUNTY_PER_DAY = 1000  # 1000+ records per county per day
FILES_PER_DAY = 2  # split each day into multiple CSV files

LOCAL_TMP_DIR = "tmp_csv"
os.makedirs(LOCAL_TMP_DIR, exist_ok=True)

# Valid codes used by your cleansing script
VALID_CODES = {"B50", "A00", "J10", "A01", "A90"}

# ============================
# DISEASE CONFIG
# ============================

# Your original diseases (valid codes)
BASE_DISEASES = {
    "Malaria": "B50",
    "Cholera": "A00",
    "Influenza": "J10",
    "Typhoid": "A01",
    "Dengue": "A90",
}

# Add 3 more diseases with INVALID codes (to simulate cleanup)
# Pick realistic examples; codes chosen to NOT be in VALID_CODES
EXTRA_INVALID_DISEASES = {
    "Measles": "B05",           # NOT in valid list
    "Tuberculosis": "A15",      # NOT in valid list
    "COVID-19": "U07.1",        # NOT in valid list
}

# Combined disease map
DISEASE_CODE_MAP = {**BASE_DISEASES, **EXTRA_INVALID_DISEASES}
DISEASES = list(DISEASE_CODE_MAP.keys())

# Outbreak-skewed probabilities:
# Malaria highest, then Influenza, then Typhoid.
# Keep some small background for other diseases, including invalid ones.
DISEASE_PROB = {
    "Malaria": 0.57,
    "Influenza": 0.20,
    "Typhoid": 0.12,
    "Cholera": 0.03,
    "Dengue": 0.03,

    # invalid-code diseases (to be cleaned out)
    "Measles": 0.02,
    "Tuberculosis": 0.02,
    "COVID-19": 0.01,
}

# Safety check: probabilities must sum to 1.0
prob_sum = sum(DISEASE_PROB.values())
if abs(prob_sum - 1.0) > 1e-9:
    raise ValueError(f"DISEASE_PROB must sum to 1.0, but got {prob_sum}")

PROB_VECTOR = [DISEASE_PROB[d] for d in DISEASES]

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
    # Creates a sequence ending "today" (last day is today)
    day = today - timedelta(days=(NUM_DAYS - 1 - day_offset))

    for file_idx in range(FILES_PER_DAY):
        records = []

        per_file_per_county = RECORDS_PER_COUNTY_PER_DAY // FILES_PER_DAY
        for county in COUNTIES:
            for rec_id in range(per_file_per_county):
                patient_id = f"{county[:3].upper()}{day_offset:02d}{file_idx}{rec_id:05d}"
                hospital = np.random.choice([
                    f"{county} General Hospital",
                    f"{county} Medical Center",
                    f"{county} Clinic"
                ])
                visit_date = day.strftime("%Y-%m-%d")

                disease = np.random.choice(DISEASES, p=PROB_VECTOR)
                diagnosis_code = DISEASE_CODE_MAP[disease]

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

        # Upload to MinIO raw bucket
        client.fput_object(MINIO_BUCKET, filename, local_path)
        print(f"Uploaded {filename} ({len(records)} records) to bucket '{MINIO_BUCKET}'")

print("All synthetic CSV data files generated and uploaded to MinIO!")
