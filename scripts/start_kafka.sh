#!/bin/bash
# Script khởi động Kafka

echo "Đang khởi động Kafka..."

cd "$(dirname "$0")/.."

# Kiểm tra Docker
if ! command -v docker &> /dev/null; then
    echo "Lỗi: Docker chưa được cài đặt"
    exit 1
fi

# Khởi động Kafka
docker-compose up -d

echo "Đang đợi Kafka sẵn sàng..."
sleep 10

# Kiểm tra Kafka
if docker ps | grep -q kafka; then
    echo "Kafka đã khởi động thành công!"
    echo "Kafka UI có thể truy cập tại: http://192.168.80.157:8080"
else
    echo "Lỗi: Kafka chưa khởi động được"
    exit 1
fi

