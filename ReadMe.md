Access URLs
Service URL
MinIO http://localhost:9001

Spark UI http://localhost:8080

Trino http://localhost:8085

Superset http://localhost:8088

docker compose exec superset \
 superset fab create-admin \
 --username admin \
 --firstname Superset \
 --lastname Admin \
 --email admin@superset.com \
 --password admin

Why Parquet format?
Parquet is a columnar storage format widely used in big data analytics for its exceptional efficiency in storage and query performance. It reduces storage size by up to 87% and improves query speeds compared to row-based formats like CSV, while supporting complex data types and schema evolution.

docker-compose restart trino

################################ Trino #####################################

# getinto trino

docker exec -it trino trino --server localhost:8080

#create schema
CREATE SCHEMA IF NOT EXISTS hive.public_health
WITH (location = 's3a://curated/');

SHOW SCHEMAS FROM hive;

CREATE TABLE IF NOT EXISTS hive.public_health.patient_visits (
patient_id varchar,
hospital varchar,
county varchar,
visit_date date,
disease varchar,
diagnosis_code varchar
)
WITH (
external_location = 's3a://curated/',
format = 'PARQUET'
);

# Test

SELECT \* FROM hive.public_health.patient_visits LIMIT 10;

# ###################### Superset

trino://admin@trino:8080/hive/public_health

# ERROR: Could not load database driver: TrinoEngineSpec

docker exec -it superset bash -lc "pip show superset-trino sqlalchemy-trino trino || true"

docker exec -it superset bash -lc "pip install --no-cache-dir superset-trino sqlalchemy-trino trino"
docker restart superset
# minio_data_lake
