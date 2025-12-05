#!/bin/bash
# Script dừng Kafka

echo "Đang dừng Kafka..."

cd "$(dirname "$0")/.."

docker-compose down

echo "Kafka đã được dừng"

