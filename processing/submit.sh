#!/bin/sh
set -eu

SQL_DIR="$(dirname "$0")/sql"
COMBINED=/tmp/flink_pipeline.sql

cat "$SQL_DIR"/01_sources.sql "$SQL_DIR"/02_transforms.sql > "$COMBINED"

python3 - "$SQL_DIR" "$COMBINED" <<'EOF'
import sys
sql_dir, combined_path = sys.argv[1], sys.argv[2]
with open(f"{sql_dir}/03_sinks.sql") as f:
    sinks = f.read()

marker = "INSERT INTO raw_events_sink"
idx = sinks.index(marker)
head, tail = sinks[:idx], sinks[idx:].rstrip()
if tail.endswith(";"):
    tail = tail[:-1]

with open(combined_path, "a") as f:
    f.write(head)
    f.write("EXECUTE STATEMENT SET\nBEGIN\n")
    f.write(tail)
    f.write(";\nEND;\n")
EOF

sed -i \
    -e "s/\${KAFKA_TOPIC}/${KAFKA_TOPIC}/g" \
    -e "s/\${KAFKA_BOOTSTRAP}/${KAFKA_BOOTSTRAP}/g" \
    -e "s/\${CLICKHOUSE_HOST}/${CLICKHOUSE_HOST}/g" \
    "$COMBINED"

docker cp "$COMBINED" docker-flink-jobmanager-1:/tmp/pipeline.sql
docker exec docker-flink-jobmanager-1 /opt/flink/bin/sql-client.sh -f /tmp/pipeline.sql
