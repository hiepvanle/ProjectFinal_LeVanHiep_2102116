#!/bin/bash
# Script setup môi trường

echo "=========================================="
echo "SETUP MÔI TRƯỜNG HEART FAILURE PREDICTION"
echo "=========================================="

# Tạo các thư mục cần thiết
echo "Đang tạo các thư mục..."
mkdir -p data
mkdir -p models
mkdir -p logs
mkdir -p checkpoints/streaming
mkdir -p airflow/dags
mkdir -p airflow/logs
mkdir -p airflow/plugins

# Cài đặt Python dependencies
echo "Đang cài đặt Python dependencies..."
pip3 install -r requirements.txt

# Kiểm tra Docker
echo "Kiểm tra Docker..."
if ! command -v docker &> /dev/null; then
    echo "Cảnh báo: Docker chưa được cài đặt"
else
    echo "Docker đã sẵn sàng"
fi

# Kiểm tra Spark
echo "Kiểm tra Spark..."
if [ -z "$SPARK_HOME" ]; then
    echo "Cảnh báo: SPARK_HOME chưa được thiết lập"
    echo "Vui lòng thiết lập biến môi trường SPARK_HOME"
else
    echo "Spark đã sẵn sàng tại: $SPARK_HOME"
fi

# Kiểm tra Airflow
echo "Kiểm tra Airflow..."
if ! command -v airflow &> /dev/null; then
    echo "Cảnh báo: Airflow chưa được cài đặt"
else
    echo "Airflow đã sẵn sàng"
fi

echo ""
echo "=========================================="
echo "HOÀN THÀNH SETUP"
echo "=========================================="

