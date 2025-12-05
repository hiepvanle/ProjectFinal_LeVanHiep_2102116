#!/bin/bash
# Script kiểm tra kết nối giữa các máy

echo "=========================================="
echo "KIỂM TRA KẾT NỐI HỆ THỐNG"
echo "=========================================="

# Đọc cấu hình
source config.py 2>/dev/null || {
    echo "Nhập thông tin cấu hình:"
    read -p "Kafka Server IP: " KAFKA_IP
    read -p "Spark Server IP: " SPARK_IP
    read -p "Airflow Server IP: " AIRFLOW_IP
}

KAFKA_IP=${KAFKA_IP:-localhost}
SPARK_IP=${SPARK_IP:-localhost}
AIRFLOW_IP=${AIRFLOW_IP:-localhost}

# Kiểm tra Kafka
echo ""
echo "1. Kiểm tra Kafka..."
if timeout 2 bash -c "cat < /dev/null > /dev/tcp/$KAFKA_IP/9092" 2>/dev/null; then
    echo "   ✓ Kafka đang chạy tại $KAFKA_IP:9092"
else
    echo "   ✗ Không thể kết nối đến Kafka tại $KAFKA_IP:9092"
fi

# Kiểm tra Zookeeper
if timeout 2 bash -c "cat < /dev/null > /dev/tcp/$KAFKA_IP/2181" 2>/dev/null; then
    echo "   ✓ Zookeeper đang chạy tại $KAFKA_IP:2181"
else
    echo "   ✗ Không thể kết nối đến Zookeeper tại $KAFKA_IP:2181"
fi

# Kiểm tra Spark
echo ""
echo "2. Kiểm tra Spark..."
if timeout 2 bash -c "cat < /dev/null > /dev/tcp/$SPARK_IP/8080" 2>/dev/null; then
    echo "   ✓ Spark UI đang chạy tại $SPARK_IP:8080"
else
    echo "   ✗ Không thể kết nối đến Spark UI tại $SPARK_IP:8080"
fi

if timeout 2 bash -c "cat < /dev/null > /dev/tcp/$SPARK_IP/7077" 2>/dev/null; then
    echo "   ✓ Spark Master đang chạy tại $SPARK_IP:7077"
else
    echo "   ✗ Không thể kết nối đến Spark Master tại $SPARK_IP:7077"
fi

# Kiểm tra Airflow
echo ""
echo "3. Kiểm tra Airflow..."
if timeout 2 bash -c "cat < /dev/null > /dev/tcp/$AIRFLOW_IP/8080" 2>/dev/null; then
    echo "   ✓ Airflow đang chạy tại $AIRFLOW_IP:8080"
else
    echo "   ✗ Không thể kết nối đến Airflow tại $AIRFLOW_IP:8080"
fi

# Kiểm tra Python dependencies
echo ""
echo "4. Kiểm tra Python dependencies..."
python3 -c "import pyspark" 2>/dev/null && echo "   ✓ PySpark" || echo "   ✗ PySpark chưa cài đặt"
python3 -c "import kafka" 2>/dev/null && echo "   ✓ Kafka-Python" || echo "   ✗ Kafka-Python chưa cài đặt"
python3 -c "import pandas" 2>/dev/null && echo "   ✓ Pandas" || echo "   ✗ Pandas chưa cài đặt"
python3 -c "import matplotlib" 2>/dev/null && echo "   ✓ Matplotlib" || echo "   ✗ Matplotlib chưa cài đặt"

# Kiểm tra Docker
echo ""
echo "5. Kiểm tra Docker..."
if command -v docker &> /dev/null; then
    echo "   ✓ Docker đã cài đặt"
    if docker ps &> /dev/null; then
        echo "   ✓ Docker đang chạy"
    else
        echo "   ✗ Docker không chạy được"
    fi
else
    echo "   ✗ Docker chưa cài đặt"
fi

# Kiểm tra Spark
echo ""
echo "6. Kiểm tra Spark..."
if [ -n "$SPARK_HOME" ]; then
    echo "   ✓ SPARK_HOME=$SPARK_HOME"
    if [ -f "$SPARK_HOME/bin/spark-submit" ]; then
        echo "   ✓ Spark đã cài đặt"
    else
        echo "   ✗ Spark chưa được cài đặt đúng"
    fi
else
    echo "   ✗ SPARK_HOME chưa được thiết lập"
fi

echo ""
echo "=========================================="
echo "HOÀN THÀNH KIỂM TRA"
echo "=========================================="

