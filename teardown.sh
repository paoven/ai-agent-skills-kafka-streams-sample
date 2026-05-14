#!/bin/bash
set -e

BOOTSTRAP="localhost:9092"

echo "Deleting topics..."
docker exec broker kafka-topics --delete --topic customers --bootstrap-server "$BOOTSTRAP" 2>/dev/null || true
docker exec broker kafka-topics --delete --topic orders --bootstrap-server "$BOOTSTRAP" 2>/dev/null || true
docker exec broker kafka-topics --delete --topic enriched-orders --bootstrap-server "$BOOTSTRAP" 2>/dev/null || true
echo "Topics deleted."

echo "Cleaning local state..."
rm -rf /tmp/kafka-streams/order-processor
echo "Done."
