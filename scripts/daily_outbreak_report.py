import trino
import pandas as pd
from datetime import datetime

# ============================
# CONFIGURATION
# ============================

TRINO_HOST = "localhost"
TRINO_PORT = 8085
TRINO_CATALOG = "public_health"
TRINO_SCHEMA = "default"
TRINO_USER = "admin"

OUTPUT_FILE = f"outbreak_report_{datetime.now().strftime('%Y%m%d')}.csv"

# ============================
# CONNECT TO TRINO
# ============================

conn = trino.dbapi.connect(
    host=TRINO_HOST,
    port=TRINO_PORT,
    user=TRINO_USER,
    catalog=TRINO_CATALOG,
    schema=TRINO_SCHEMA,
)

cursor = conn.cursor()

# ============================
# OUTBREAK DETECTION QUERY
# ============================

query = """
WITH daily_counts AS (
    SELECT
        county,
        disease,
        visit_date,
        COUNT(*) AS cases
    FROM patients
    GROUP BY county, disease, visit_date
),
rolling_sum AS (
    SELECT
        county,
        disease,
        visit_date,
        SUM(cases) OVER (
            PARTITION BY county, disease
            ORDER BY visit_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS cases_last_7_days,
        LAG(SUM(cases) OVER (
            PARTITION BY county, disease
            ORDER BY visit_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )) OVER (PARTITION BY county, disease ORDER BY visit_date) AS prev_7_day
    FROM daily_counts
)
SELECT
    county,
    disease,
    visit_date,
    cases_last_7_days
FROM rolling_sum
WHERE prev_7_day IS NOT NULL
  AND cases_last_7_days > prev_7_day * 1.5
ORDER BY visit_date, county, disease
"""

# ============================
# RUN QUERY AND FETCH RESULTS
# ============================

cursor.execute(query)
rows = cursor.fetchall()

# Convert to DataFrame
columns = [col[0] for col in cursor.description]
df = pd.DataFrame(rows, columns=columns)

# ============================
# SAVE TO CSV
# ============================

df.to_csv(OUTPUT_FILE, index=False)
print(f"Daily outbreak report saved to {OUTPUT_FILE}")
