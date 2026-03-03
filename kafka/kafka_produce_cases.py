import os
import json
import time
import uuid
import random
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

# ============================
# CONFIG
# ============================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "public_health_visits")

# How long to simulate
NUM_DAYS = int(os.getenv("NUM_DAYS", "30"))

# Rate control
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "20"))  # e.g. 20 events/sec
TOTAL_EVENTS = int(os.getenv("TOTAL_EVENTS", "0"))  # 0 = run forever (until Ctrl+C)

COUNTIES = os.getenv("COUNTIES", "Nairobi,Mombasa,Kisumu,Nakuru,Eldoret").split(",")

# Base diseases (valid codes)
BASE_DISEASES = {
    "Malaria": "B50",
    "Cholera": "A00",
    "Influenza": "J10",
    "Typhoid": "A01",
    "Dengue": "A90",
}

# Invalid-code diseases (for cleanup simulation) - codes NOT in valid list
INVALID_DISEASES = {
    "Measles": "B05",
    "Tuberculosis": "A15",
    "COVID-19": "U07.1",
}

INCLUDE_INVALID = os.getenv("INCLUDE_INVALID", "true").lower() in ("1", "true", "yes")

DISEASE_CODE_MAP = dict(BASE_DISEASES)
if INCLUDE_INVALID:
    DISEASE_CODE_MAP.update(INVALID_DISEASES)

DISEASES = list(DISEASE_CODE_MAP.keys())

# Outbreak skew: Malaria > Influenza > Typhoid
# Keep small probability for others (including invalid)
DEFAULT_PROBS = {
    "Malaria": 0.45,
    "Influenza": 0.27,
    "Typhoid": 0.12,
    "Cholera": 0.08,
    "Dengue": 0.03,
    "Measles": 0.02,
    "Tuberculosis": 0.02,
    "COVID-19": 0.01,
}

# Build probability vector only for included diseases, then renormalize
probs = []
for d in DISEASES:
    probs.append(DEFAULT_PROBS.get(d, 0.0))
s = sum(probs)
if s <= 0:
    raise ValueError("Disease probabilities sum to 0. Check DEFAULT_PROBS / INCLUDE_INVALID.")
probs = [p / s for p in probs]

# Create visit_date distribution across the last NUM_DAYS
today = datetime.now(timezone.utc).date()
date_choices = [today - timedelta(days=i) for i in range(NUM_DAYS)]
# Bias slightly towards recent days
date_weights = [1.0 + (NUM_DAYS - 1 - i) * 0.02 for i in range(NUM_DAYS)]  # mild ramp
dw_sum = sum(date_weights)
date_weights = [w / dw_sum for w in date_weights]

def rand_hospital(county: str) -> str:
    return random.choice([
        f"{county} General Hospital",
        f"{county} Medical Center",
        f"{county} Clinic",
        f"{county} County Referral Hospital",
    ])

def make_event() -> dict:
    county = random.choice(COUNTIES)
    disease = random.choices(DISEASES, weights=probs, k=1)[0]
    diagnosis_code = DISEASE_CODE_MAP[disease]

    visit_date = random.choices(date_choices, weights=date_weights, k=1)[0].isoformat()

    # Make patient_id unique-ish
    patient_id = f"{county[:3].upper()}-{uuid.uuid4().hex[:10]}"

    return {
        "patient_id": patient_id,
        "hospital": rand_hospital(county),
        "county": county,
        "visit_date": visit_date,   # yyyy-mm-dd
        "disease": disease,
        "diagnosis_code": diagnosis_code,
        "event_time_utc": datetime.now(timezone.utc).isoformat()
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=5,
        retries=5,
    )

    interval = 1.0 / EVENTS_PER_SECOND if EVENTS_PER_SECOND > 0 else 0.0
    sent = 0

    print(f"Producing to Kafka topic='{KAFKA_TOPIC}' bootstrap='{KAFKA_BOOTSTRAP}'")
    print(f"Rate: {EVENTS_PER_SECOND} events/sec | Days simulated: {NUM_DAYS} | Include invalid: {INCLUDE_INVALID}")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            evt = make_event()
            producer.send(KAFKA_TOPIC, evt)
            sent += 1

            if sent % 500 == 0:
                producer.flush()
                print(f"Sent {sent} events...")

            if TOTAL_EVENTS > 0 and sent >= TOTAL_EVENTS:
                break

            if interval > 0:
                time.sleep(interval)

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()
        producer.close()
        print(f"Done. Total sent: {sent}")

if __name__ == "__main__":
    main()