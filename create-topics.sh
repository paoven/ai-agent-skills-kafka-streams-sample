#!/bin/bash
set -e

BOOTSTRAP="localhost:9092"

echo "Creating topics..."
docker exec broker kafka-topics --create --if-not-exists --topic customers --partitions 4 --replication-factor 1 --bootstrap-server "$BOOTSTRAP"
docker exec broker kafka-topics --create --if-not-exists --topic orders --partitions 4 --replication-factor 1 --bootstrap-server "$BOOTSTRAP"
docker exec broker kafka-topics --create --if-not-exists --topic enriched-orders --partitions 4 --replication-factor 1 --bootstrap-server "$BOOTSTRAP"
echo "Topics created."
