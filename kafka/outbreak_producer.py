import os
import json
import time
import uuid
import random
import math
from datetime import datetime, timedelta, timezone, date
from kafka import KafkaProducer

# ============================
# CONFIG
# ============================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "public_health_visits")

NUM_DAYS = int(os.getenv("NUM_DAYS", "30"))  # recommend 30+
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "20"))
TOTAL_EVENTS = int(os.getenv("TOTAL_EVENTS", "0"))  # 0 = run forever

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

# Baseline probabilities (global skew)
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

# County baseline multipliers (steady bias)
COUNTY_BASE_MULTIPLIERS = {
    "Mombasa": {"Malaria": 1.6},
    "Nairobi": {"Cholera": 1.8},
}

# ============================
# REALISTIC OUTBREAK WAVES (bell curve)
# ============================
# Each wave applies a smooth multiplier:
# multiplier = 1 + amplitude * exp(-0.5 * ((day - peak)/sigma)^2)
#
# where:
# - amplitude controls peak strength (e.g., 4.0 => up to ~5x at peak)
# - peak_offset_days: 0 means peak today, 3 means peak 3 days ago, -2 peak 2 days in future
# - sigma: spread (larger => longer outbreak)
#
# You can define multiple waves.
WAVES = [
    {
        "name": "Mombasa Malaria wave",
        "county": "Mombasa",
        "disease": "Malaria",
        "amplitude": float(os.getenv("MOMBASA_MALARIA_WAVE_AMP", "4.0")),
        "peak_offset_days": int(os.getenv("MOMBASA_MALARIA_PEAK_OFFSET", "2")),  # peak 2 days ago
        "sigma_days": float(os.getenv("MOMBASA_MALARIA_SIGMA", "5.0")),          # broader wave
    },
    {
        "name": "Nairobi Cholera wave",
        "county": "Nairobi",
        "disease": "Cholera",
        "amplitude": float(os.getenv("NAIROBI_CHOLERA_WAVE_AMP", "5.0")),
        "peak_offset_days": int(os.getenv("NAIROBI_CHOLERA_PEAK_OFFSET", "1")),  # peak 1 day ago
        "sigma_days": float(os.getenv("NAIROBI_CHOLERA_SIGMA", "3.0")),          # sharper wave
    },
]

# Optional: dampen other diseases slightly during strong wave days (makes spike clearer)
DAMPEN_OTHERS = os.getenv("DAMPEN_OTHERS_DURING_WAVE", "true").lower() in ("1", "true", "yes")
DAMPEN_FACTOR_MIN = float(os.getenv("DAMPEN_FACTOR_MIN", "0.80"))  # strongest dampening near peak
DAMPEN_FACTOR_MAX = float(os.getenv("DAMPEN_FACTOR_MAX", "1.00"))  # no dampening far from peak

# ============================
# DATE CHOICES (last NUM_DAYS) with mild recent bias
# ============================
today = datetime.now(timezone.utc).date()
date_choices = [today - timedelta(days=i) for i in range(NUM_DAYS)]
date_weights = [1.0 + (NUM_DAYS - 1 - i) * 0.03 for i in range(NUM_DAYS)]
dw_sum = sum(date_weights)
date_weights = [w / dw_sum for w in date_weights]

def normalize(weights: dict) -> dict:
    s = sum(weights.values())
    if s <= 0:
        raise ValueError("Weights sum to 0.")
    return {k: v / s for k, v in weights.items()}

def gaussian_multiplier(day: date, peak_day: date, amp: float, sigma: float) -> float:
    if sigma <= 0:
        return 1.0
    x = (day - peak_day).days / sigma
    return 1.0 + amp * math.exp(-0.5 * x * x)

def wave_strength_0_to_1(day: date, peak_day: date, sigma: float) -> float:
    """
    A 0..1 measure for how close we are to peak (1 at peak).
    Useful for dampening other diseases slightly near peak.
    """
    if sigma <= 0:
        return 0.0
    x = (day - peak_day).days / sigma
    return math.exp(-0.5 * x * x)

def build_probs_for(county: str, visit_day: date) -> list[float]:
    # Baseline weights (not normalized)
    weights = {d: float(DEFAULT_PROBS.get(d, 0.0)) for d in DISEASES}

    # Apply steady county multipliers
    base_mults = COUNTY_BASE_MULTIPLIERS.get(county, {})
    for d, m in base_mults.items():
        if d in weights:
            weights[d] *= float(m)

    # Apply bell-curve waves
    # Also compute max wave closeness for dampening others
    max_strength = 0.0

    for w in WAVES:
        if w["county"] != county:
            continue
        disease = w["disease"]
        if disease not in weights:
            continue

        peak_day = today - timedelta(days=int(w["peak_offset_days"]))
        mult = gaussian_multiplier(
            day=visit_day,
            peak_day=peak_day,
            amp=float(w["amplitude"]),
            sigma=float(w["sigma_days"]),
        )
        weights[disease] *= mult

        # Track closeness to peak for optional dampening
        max_strength = max(max_strength, wave_strength_0_to_1(visit_day, peak_day, float(w["sigma_days"])))

    # Optional dampening of other diseases near peak to make surge clearer
    if DAMPEN_OTHERS and max_strength > 0:
        # Interpolate dampening factor based on wave strength:
        # near peak -> DAMPEN_FACTOR_MIN, far -> DAMPEN_FACTOR_MAX
        damp = DAMPEN_FACTOR_MAX - (DAMPEN_FACTOR_MAX - DAMPEN_FACTOR_MIN) * max_strength
        # Find which diseases are "wave diseases" for this county to exclude from dampening
        wave_diseases = {w["disease"] for w in WAVES if w["county"] == county}
        for d in weights:
            if d not in wave_diseases:
                weights[d] *= damp

    weights = normalize(weights)
    return [weights[d] for d in DISEASES]

def rand_hospital(county: str) -> str:
    return random.choice([
        f"{county} General Hospital",
        f"{county} Medical Center",
        f"{county} Clinic",
        f"{county} County Referral Hospital",
    ])

def make_event() -> dict:
    county = random.choice(COUNTIES)

    visit_day = random.choices(date_choices, weights=date_weights, k=1)[0]
    probs = build_probs_for(county, visit_day)

    disease = random.choices(DISEASES, weights=probs, k=1)[0]
    diagnosis_code = DISEASE_CODE_MAP[disease]

    patient_id = f"{county[:3].upper()}-{uuid.uuid4().hex[:10]}"

    return {
        "patient_id": patient_id,
        "hospital": rand_hospital(county),
        "county": county,
        "visit_date": visit_day.isoformat(),
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
    print("County baseline multipliers:", json.dumps(COUNTY_BASE_MULTIPLIERS, indent=2))
    print("Waves:", json.dumps(WAVES, indent=2))
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