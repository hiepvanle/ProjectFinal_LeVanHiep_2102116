# TÓM TẮT DỰ ÁN

## Tên Dự án
Heart Failure Prediction Pipeline với Spark ML, Kafka và Airflow

## Mô tả Ngắn gọn
Xây dựng hệ thống end-to-end để dự đoán Heart Failure sử dụng Spark ML cho machine learning, Kafka cho streaming data, và Airflow để điều phối toàn bộ workflow.

## Các Thành phần Chính

### 1. Data Processing
- **File**: `data_split.py`
- **Chức năng**: Chia dữ liệu thành train (70%) và stream (30%)

### 2. Model Training
- **File**: `spark_training.py`
- **Algorithm**: Random Forest Classifier
- **Output**: PipelineModel tại `models/heart_failure_model`

### 3. Streaming Infrastructure
- **Kafka**: Docker Compose setup (`docker-compose.yml`)
- **Topics**: 
  - `heart_failure_input` (input)
  - `heart_failure_predictions` (output)

### 4. Stream Simulation
- **File**: `stream_simulator.py`
- **Chức năng**: Gửi dữ liệu streaming đến Kafka

### 5. Spark Streaming
- **File**: `spark_streaming.py`
- **Chức năng**: Đọc từ Kafka, dự đoán, ghi kết quả về Kafka

### 6. Visualization
- **File**: `visualization_consumer.py`
- **Chức năng**: Trực quan hóa kết quả real-time

### 7. Orchestration
- **File**: `airflow/dags/heart_failure_pipeline.py`
- **Chức năng**: Điều phối toàn bộ pipeline

## Cấu trúc Thư mục

```
PROJECTFINAL_LEVANHIEP_2102116/
├── data/                          # Dữ liệu (train, stream)
├── models/                        # Mô hình đã huấn luyện
├── logs/                          # Log files
├── checkpoints/                   # Spark checkpoints
├── airflow/dags/                  # Airflow DAGs
├── scripts/                       # Helper scripts
├── data_split.py                 # Chia dữ liệu
├── spark_training.py             # Huấn luyện mô hình
├── spark_streaming.py            # Spark Streaming
├── stream_simulator.py           # Stream simulator
├── visualization_consumer.py     # Visualization
├── docker-compose.yml            # Kafka setup
├── requirements.txt              # Dependencies
├── README.md                     # Tài liệu chính
├── DEPLOYMENT.md                 # Hướng dẫn triển khai
├── QUICKSTART.md                 # Hướng dẫn nhanh
└── BAO_CAO.md                    # Báo cáo chi tiết
```

## Yêu cầu Hệ thống

- Python 3.8+
- Spark 3.3.0+
- Docker & Docker Compose
- Apache Airflow 2.7.0+
- Java 8+ (cho Spark)

## Cách Chạy

### Single Machine
```bash
./scripts/setup.sh
./scripts/start_kafka.sh
python3 data_split.py
$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 spark_training.py
# Chạy spark_streaming.py, stream_simulator.py, visualization_consumer.py
```

### Với Airflow
1. Khởi động Airflow webserver và scheduler
2. Truy cập UI tại http://localhost:8080
3. Trigger DAG `heart_failure_pipeline`

## Kết quả Mong đợi

- Model AUC Score: ~0.85-0.90
- Real-time predictions với latency < 5 giây
- Visualization real-time của predictions

## Tác giả
Lê Văn Hiệp - 2102116

