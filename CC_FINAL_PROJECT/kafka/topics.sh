#!/bin/bash
docker exec kafka kafka-topics.sh --create \
  --topic api-logs \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1

