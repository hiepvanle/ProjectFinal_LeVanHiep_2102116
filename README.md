# Heart Failure Prediction Pipeline với Spark ML, Kafka và Airflow

Dự án này xây dựng một pipeline hoàn chỉnh để dự đoán Heart Failure sử dụng Spark ML, Kafka cho streaming và Airflow để điều phối toàn bộ quá trình.

## Kiến trúc hệ thống

```
┌─────────────┐
│   Airflow   │ ── Điều phối toàn bộ pipeline
└──────┬──────┘
       │
       ├──> Khởi động Kafka (Docker)
       ├──> Chia dữ liệu
       ├──> Huấn luyện mô hình Spark ML
       ├──> Chạy Spark Streaming
       └──> Chạy Stream Simulator
       
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   Kafka     │ <── │ Spark        │ ──> │ Kafka       │
│  (Input)    │     │ Streaming    │     │ (Output)    │
└─────────────┘     └──────────────┘     └─────────────┘
       ▲                    │                    │
       │                    │                    │
       │                    ▼                    │
       │            ┌──────────────┐             │
       │            │ Spark ML     │             │
       │            │ Model        │             │
       │            └──────────────┘             │
       │                                          │
       └──────────────────────────────────────────┘
                    │
                    ▼
            ┌──────────────┐
            │ Visualization│
            │ Consumer     │
            └──────────────┘
```

## Yêu cầu hệ thống

### Máy 1: Spark Server
- Spark 3.3.0+
- Java 8+
- Python 3.8+
- Cấu hình: SPARK_HOME environment variable

### Máy 2: Airflow Server
- Apache Airflow 2.7.0+
- Python 3.8+
- Docker & Docker Compose
- Quyền truy cập SSH đến các máy khác (nếu cần)

### Máy 3: Kafka Server
- Docker & Docker Compose
- Ports: 9092 (Kafka), 2181 (Zookeeper), 8080 (Kafka UI)

### Máy Demo: Streaming & Visualization
- Python 3.8+
- Kafka Python client
- Matplotlib cho visualization

## Cài đặt

### 1. Clone repository và setup môi trường

```bash
cd PROJECTFINAL_LEVANHIEP_2102116
chmod +x scripts/*.sh
./scripts/setup.sh
```

### 2. Cài đặt dependencies

```bash
pip3 install -r requirements.txt
```

### 3. Cấu hình môi trường

Thiết lập các biến môi trường:

```bash
export SPARK_HOME=/opt/spark  # Đường dẫn đến Spark
export KAFKA_SERVERS=kafka-server-ip:9092  # IP máy Kafka
```

## Sử dụng

### Cách 1: Chạy thủ công từng bước

#### Bước 1: Khởi động Kafka

```bash
./scripts/start_kafka.sh
```

Hoặc trên máy Kafka:
```bash
docker-compose up -d
```

#### Bước 2: Chia dữ liệu

```bash
python3 data_split.py
```

#### Bước 3: Huấn luyện mô hình

Trên máy Spark:
```bash
python3 spark_training.py data/train_data.csv models/heart_failure_model
```

Hoặc sử dụng spark-submit:
```bash
$SPARK_HOME/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
  spark_training.py \
  data/train_data.csv \
  models/heart_failure_model
```

#### Bước 4: Chạy Spark Streaming

Trên máy Spark:
```bash
python3 spark_streaming.py
```

Hoặc chỉ định các tham số:
```bash
python3 spark_streaming.py \
  models/heart_failure_model \
  heart_failure_input \
  heart_failure_predictions \
  192.168.80.88:9092
```

Lưu ý: Mặc định sử dụng Kafka broker tại `192.168.80.88:9092`

#### Bước 5: Chạy Stream Simulator

Trên máy demo:
```bash
python3 stream_simulator.py \
  --stream-file data/stream_data.csv \
  --kafka-topic heart_failure_input \
  --bootstrap-servers kafka-server-ip:9092 \
  --delay 2
```

#### Bước 6: Visualization

Trên máy demo:
```bash
python3 visualization_consumer.py \
  --kafka-topic heart_failure_predictions \
  --bootstrap-servers kafka-server-ip:9092
```

### Cách 2: Sử dụng Airflow (Khuyến nghị)

#### Bước 1: Khởi động Airflow

```bash
# Khởi tạo database
airflow db init

# Tạo user
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin

# Khởi động webserver
airflow webserver --port 8080 -D

# Khởi động scheduler
airflow scheduler -D
```

#### Bước 2: Cấu hình DAG

Chỉnh sửa file `airflow/dags/heart_failure_pipeline.py` để cập nhật:
- `PROJECT_DIR`: Đường dẫn dự án
- `SPARK_HOME`: Đường dẫn Spark
- `KAFKA_SERVERS`: Địa chỉ Kafka brokers

#### Bước 3: Chạy DAG

1. Truy cập Airflow UI: http://localhost:8080
2. Tìm DAG `heart_failure_pipeline`
3. Kích hoạt và trigger DAG

## Cấu trúc thư mục

```
PROJECTFINAL_LEVANHIEP_2102116/
├── data/
│   ├── train_data.csv          # Dữ liệu huấn luyện
│   └── stream_data.csv          # Dữ liệu streaming
├── models/
│   └── heart_failure_model/     # Mô hình đã huấn luyện
├── logs/                        # Log files
├── checkpoints/                 # Spark checkpoints
├── airflow/
│   └── dags/
│       └── heart_failure_pipeline.py
├── scripts/
│   ├── setup.sh
│   ├── start_kafka.sh
│   └── stop_kafka.sh
├── data_split.py                # Chia dữ liệu
├── spark_training.py            # Huấn luyện mô hình
├── spark_streaming.py           # Spark Streaming
├── stream_simulator.py          # Mô phỏng streaming
├── visualization_consumer.py    # Trực quan hóa
├── docker-compose.yml           # Kafka setup
├── requirements.txt             # Python dependencies
└── README.md                    # Tài liệu này
```

## Kafka Topics

- `heart_failure_input`: Topic nhận dữ liệu streaming từ simulator
- `heart_failure_predictions`: Topic chứa kết quả dự đoán từ Spark

## Mô hình Machine Learning

- **Algorithm**: Random Forest Classifier
- **Features**: age, anaemia, creatinine_phosphokinase, diabetes, ejection_fraction, high_blood_pressure, platelets, serum_creatinine, serum_sodium, sex, smoking
- **Target**: DEATH_EVENT (0/1)
- **Metrics**: AUC Score

## Monitoring

### Kafka UI
Truy cập http://kafka-server-ip:8080 để xem:
- Topics và messages
- Consumer groups
- Broker status

### Spark UI
Truy cập http://spark-server-ip:4040 để xem:
- Spark jobs
- Streaming queries
- Executors

### Airflow UI
Truy cập http://airflow-server-ip:8080 để xem:
- DAG execution status
- Task logs
- Pipeline history

## Troubleshooting

### Kafka không khởi động
```bash
# Kiểm tra logs
docker-compose logs kafka

# Kiểm tra ports
netstat -tulpn | grep 9092
```

### Spark không kết nối được Kafka
- Kiểm tra firewall
- Kiểm tra địa chỉ Kafka brokers
- Đảm bảo Kafka đã sẵn sàng

### Airflow không chạy được tasks
- Kiểm tra SSH connection (nếu dùng SSHOperator)
- Kiểm tra đường dẫn files
- Kiểm tra permissions

## Tác giả

Lê Văn Hiệp - 2102116

## License

MIT License

