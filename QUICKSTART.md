# Hướng dẫn Nhanh

## Chạy trên Single Machine (Development)

### 1. Setup môi trường

```bash
./scripts/setup.sh
```

### 2. Khởi động Kafka

```bash
./scripts/start_kafka.sh
```

### 3. Chia dữ liệu

```bash
python3 data_split.py
```

### 4. Huấn luyện mô hình

```bash
$SPARK_HOME/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
  spark_training.py
```

### 5. Chạy Spark Streaming (Terminal 1)

```bash
$SPARK_HOME/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
  spark_streaming.py
```

### 6. Chạy Stream Simulator (Terminal 2)

```bash
python3 stream_simulator.py
```

### 7. Visualization (Terminal 3)

```bash
python3 visualization_consumer.py
```

## Chạy với Airflow

### 1. Khởi động Airflow

```bash
# Terminal 1: Webserver
airflow webserver --port 8080

# Terminal 2: Scheduler
airflow scheduler
```

### 2. Truy cập UI

Mở browser: http://localhost:8080
- Username: admin
- Password: admin

### 3. Trigger DAG

1. Tìm DAG `heart_failure_pipeline`
2. Toggle ON
3. Click "Trigger DAG"

## Kiểm tra Status

### Kafka Topics

```bash
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Kafka Messages

```bash
# Đọc messages từ input topic
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic heart_failure_input \
  --from-beginning

# Đọc messages từ output topic
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic heart_failure_predictions \
  --from-beginning
```

### Spark UI

Truy cập: http://localhost:4040 (khi Spark đang chạy)

### Kafka UI

Truy cập: http://localhost:8080 (Kafka UI)

## Troubleshooting

### Kafka không khởi động

```bash
# Kiểm tra logs
docker-compose logs kafka

# Restart
docker-compose restart kafka
```

### Spark không kết nối Kafka

- Kiểm tra Kafka đang chạy: `docker ps`
- Kiểm tra port 9092: `netstat -tulpn | grep 9092`
- Kiểm tra firewall

### Airflow DAG không chạy

- Kiểm tra logs: `~/airflow/logs/`
- Kiểm tra DAG syntax: `airflow dags list`
- Kiểm tra task logs trong UI

