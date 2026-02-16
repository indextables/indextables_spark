#!/usr/bin/env bash
#
# Seed an Iceberg test table via PySpark in a temporary Spark container.
# Prerequisites: docker compose up -d (rest + minio must be running)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
NETWORK="iceberg-local_iceberg-net"

echo "==> Seeding Iceberg test table: default.test_events"

# Write a PySpark script that creates and populates the table
PY_FILE="$(mktemp /tmp/iceberg-seed-XXXXXX.py)"
cat > "$PY_FILE" <<'PYTHON'
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("iceberg-seed") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "rest") \
    .config("spark.sql.catalog.demo.uri", "http://rest:8181") \
    .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/") \
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.demo.s3.path-style-access", "true") \
    .getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.default")
spark.sql("DROP TABLE IF EXISTS demo.default.test_events")

spark.sql("""
CREATE TABLE demo.default.test_events (
  event_id   BIGINT,
  event_type STRING,
  user_name  STRING,
  score      DOUBLE,
  event_time TIMESTAMP,
  region     STRING
)
USING iceberg
PARTITIONED BY (region)
""")

spark.sql("""
INSERT INTO demo.default.test_events VALUES
  (1, 'click',    'alice',   0.95, TIMESTAMP '2024-01-15 10:30:00', 'us-east'),
  (2, 'purchase', 'bob',     1.50, TIMESTAMP '2024-01-15 11:00:00', 'us-east'),
  (3, 'click',    'charlie', 0.20, TIMESTAMP '2024-01-15 12:15:00', 'us-west'),
  (4, 'view',     'diana',   0.00, TIMESTAMP '2024-01-16 08:00:00', 'us-west'),
  (5, 'click',    'eve',     0.80, TIMESTAMP '2024-01-16 09:30:00', 'eu-west'),
  (6, 'purchase', 'frank',   2.10, TIMESTAMP '2024-01-16 10:00:00', 'eu-west'),
  (7, 'view',     'grace',   0.10, TIMESTAMP '2024-01-17 14:00:00', 'us-east'),
  (8, 'click',    'hank',    0.55, TIMESTAMP '2024-01-17 15:30:00', 'us-west'),
  (9, 'purchase', 'iris',    3.00, TIMESTAMP '2024-01-18 09:00:00', 'eu-west'),
  (10,'view',     'jack',    0.05, TIMESTAMP '2024-01-18 11:00:00', 'us-east')
""")

# Second insert creates a second snapshot (useful for time-travel tests)
spark.sql("""
INSERT INTO demo.default.test_events VALUES
  (11, 'click',    'kate',   0.70, TIMESTAMP '2024-02-01 10:00:00', 'us-east'),
  (12, 'purchase', 'leo',    1.80, TIMESTAMP '2024-02-01 11:00:00', 'us-west')
""")

count = spark.sql("SELECT COUNT(*) FROM demo.default.test_events").collect()[0][0]
print(f"==> Table seeded with {count} rows")

# Print snapshot info for time-travel testing
snapshots = spark.sql("SELECT snapshot_id FROM demo.default.test_events.snapshots ORDER BY committed_at").collect()
for i, s in enumerate(snapshots):
    print(f"  Snapshot {i}: {s[0]}")

spark.stop()
PYTHON

docker run --rm --network "$NETWORK" \
  --entrypoint "" \
  -v "$PY_FILE:/tmp/seed.py:ro" \
  -e AWS_ACCESS_KEY_ID=admin \
  -e AWS_SECRET_ACCESS_KEY=password \
  -e AWS_REGION=us-east-1 \
  tabulario/spark-iceberg:latest \
  spark-submit --master "local[*]" /tmp/seed.py

rm -f "$PY_FILE"

echo ""
echo "==> Done. Table default.test_events seeded with 12 rows across 2 snapshots."
echo ""
echo "Credentials for ~/.iceberg/credentials:"
echo ""
echo "[rest]"
echo "uri = http://localhost:8181"
echo "warehouse = s3://warehouse/"
echo "s3_endpoint = http://localhost:9000"
echo "s3_path_style_access = true"
echo "table = default.test_events"
