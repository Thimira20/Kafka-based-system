#!/usr/bin/env bash
# Creates the topics used by the pipeline. Run after `docker compose up -d`.
# Usage: ./scripts/create-topics.sh
set -euo pipefail

BROKER_CONTAINER=kafka
BOOTSTRAP=localhost:9092

create_topic() {
  local name=$1
  local partitions=$2
  echo "Creating topic '$name' (partitions=$partitions)..."
  docker exec "$BROKER_CONTAINER" kafka-topics \
    --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor 1
}

create_topic orders 3
create_topic orders.DLQ 1

echo
echo "Topics now on the cluster:"
docker exec "$BROKER_CONTAINER" kafka-topics --bootstrap-server "$BOOTSTRAP" --list
