# Hướng dẫn Triển khai Multi-Machine

## Kiến trúc Triển khai

```
┌─────────────────┐
│  Máy 1: Spark   │
│  - Spark Master │
│  - Spark Worker │
└─────────────────┘

┌─────────────────┐
│ Máy 2: Airflow  │
│  - Airflow      │
│  - Scheduler    │
└─────────────────┘

┌─────────────────┐
│ Máy 3: Kafka    │
│  - Zookeeper    │
│  - Kafka Broker │
│  - Kafka UI     │
└─────────────────┘

┌─────────────────┐
│ Máy Demo:       │
│  - Stream Sim   │
│  - Visualization│
└─────────────────┘
```

## Bước 1: Cài đặt trên Máy Spark

### 1.1. Cài đặt Spark

```bash
# Download Spark
wget https://archive.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz
tar -xzf spark-3.3.0-bin-hadoop3.tgz
sudo mv spark-3.3.0-bin-hadoop3 /opt/spark

# Thiết lập biến môi trường
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> ~/.bashrc
source ~/.bashrc

# Cấu hình Spark
cd $SPARK_HOME/conf
cp spark-env.sh.template spark-env.sh
echo 'export SPARK_MASTER_HOST=spark-server-ip' >> spark-env.sh
echo 'export SPARK_MASTER_PORT=7077' >> spark-env.sh
```

### 1.2. Copy project files

```bash
# Copy toàn bộ project đến máy Spark
scp -r PROJECTFINAL_LEVANHIEP_2102116 user@spark-server:/home/user/

# Cài đặt dependencies
cd /home/user/PROJECTFINAL_LEVANHIEP_2102116
pip3 install -r requirements.txt
```

### 1.3. Khởi động Spark

```bash
# Khởi động Spark Master
$SPARK_HOME/sbin/start-master.sh

# Khởi động Spark Worker (có thể chạy trên cùng máy hoặc máy khác)
$SPARK_HOME/sbin/start-worker.sh spark://spark-server-ip:7077
```

## Bước 2: Cài đặt trên Máy Kafka

### 2.1. Cài đặt Docker

```bash
# Cài đặt Docker
sudo apt-get update
sudo apt-get install -y docker.io docker-compose

# Khởi động Docker
sudo systemctl start docker
sudo systemctl enable docker

# Thêm user vào docker group
sudo usermod -aG docker $USER
```

### 2.2. Copy project files

```bash
# Copy project đến máy Kafka
scp -r PROJECTFINAL_LEVANHIEP_2102116 user@kafka-server:/home/user/
scp docker-compose.yml user@kafka-server:/home/user/PROJECTFINAL_LEVANHIEP_2102116/
```

### 2.3. Cấu hình Kafka

Chỉnh sửa `docker-compose.yml` để cập nhật advertised listeners:

```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-server-ip:9092,PLAINTEXT_INTERNAL://kafka:29092
```

### 2.4. Khởi động Kafka

```bash
cd /home/user/PROJECTFINAL_LEVANHIEP_2102116
docker-compose up -d

# Kiểm tra status
docker-compose ps
```

### 2.5. Tạo Topics

```bash
docker exec kafka kafka-topics --create \
  --topic heart_failure_input \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

docker exec kafka kafka-topics --create \
  --topic heart_failure_predictions \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# Kiểm tra topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

## Bước 3: Cài đặt trên Máy Airflow

### 3.1. Cài đặt Airflow

```bash
# Cài đặt dependencies
sudo apt-get install -y python3-pip python3-dev

# Cài đặt Airflow
pip3 install apache-airflow==2.7.0

# Khởi tạo database
export AIRFLOW_HOME=~/airflow
airflow db init

# Tạo admin user
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin
```

### 3.2. Copy project files

```bash
# Copy project đến máy Airflow
scp -r PROJECTFINAL_LEVANHIEP_2102116 user@airflow-server:/home/user/

# Copy DAG
mkdir -p ~/airflow/dags
cp PROJECTFINAL_LEVANHIEP_2102116/airflow/dags/heart_failure_pipeline.py ~/airflow/dags/
```

### 3.3. Cấu hình Airflow DAG

Chỉnh sửa `~/airflow/dags/heart_failure_pipeline.py`:

```python
PROJECT_DIR = "/home/user/PROJECTFINAL_LEVANHIEP_2102116"
SPARK_HOME = "/opt/spark"  # Đường dẫn trên máy Spark
KAFKA_SERVERS = "kafka-server-ip:9092"  # IP máy Kafka
```

**Lưu ý**: Nếu Spark và Airflow ở các máy khác nhau, bạn cần:
- Sử dụng SSH để chạy commands trên máy Spark
- Hoặc cấu hình Spark để chấp nhận remote submissions

### 3.4. Khởi động Airflow

```bash
# Khởi động webserver
airflow webserver --port 8080 -D

# Khởi động scheduler
airflow scheduler -D
```

Truy cập: http://airflow-server-ip:8080

## Bước 4: Cài đặt trên Máy Demo

### 4.1. Copy project files

```bash
# Copy project đến máy demo
scp -r PROJECTFINAL_LEVANHIEP_2102116 user@demo-machine:/home/user/

# Cài đặt dependencies
cd PROJECTFINAL_LEVANHIEP_2102116
pip3 install -r requirements.txt
```

### 4.2. Cấu hình

Chỉnh sửa các file để trỏ đến đúng IP:
- `stream_simulator.py`: `--bootstrap-servers kafka-server-ip:9092`
- `visualization_consumer.py`: `--bootstrap-servers kafka-server-ip:9092`

## Bước 5: Kiểm tra Kết nối

### 5.1. Kiểm tra Kafka

```bash
# Từ máy bất kỳ
telnet kafka-server-ip 9092
```

### 5.2. Kiểm tra Spark

```bash
# Từ máy Spark
curl http://spark-server-ip:8080
```

### 5.3. Kiểm tra Airflow

```bash
# Từ máy bất kỳ
curl http://airflow-server-ip:8080
```

## Bước 6: Chạy Pipeline

### Cách 1: Qua Airflow UI

1. Truy cập http://airflow-server-ip:8080
2. Login với admin/admin
3. Tìm DAG `heart_failure_pipeline`
4. Toggle ON để kích hoạt
5. Click "Trigger DAG"

### Cách 2: Command Line

```bash
# Từ máy Airflow
airflow dags trigger heart_failure_pipeline
```

## Bước 7: Chạy Visualization

Từ máy demo:

```bash
cd PROJECTFINAL_LEVANHIEP_2102116
python3 visualization_consumer.py \
  --kafka-topic heart_failure_predictions \
  --bootstrap-servers kafka-server-ip:9092
```

## Troubleshooting

### Lỗi kết nối Kafka

```bash
# Kiểm tra firewall
sudo ufw allow 9092/tcp
sudo ufw allow 2181/tcp

# Kiểm tra Kafka logs
docker logs kafka
```

### Lỗi Spark không submit được

- Kiểm tra SPARK_HOME
- Kiểm tra network connectivity
- Kiểm tra Spark Master đang chạy

### Lỗi Airflow không chạy được tasks

- Kiểm tra đường dẫn files
- Kiểm tra permissions
- Kiểm tra logs: `~/airflow/logs/`

## Firewall Rules

Mở các ports sau:

**Máy Spark:**
- 7077: Spark Master
- 8080: Spark Master UI
- 8081: Spark Worker UI

**Máy Kafka:**
- 9092: Kafka Broker
- 2181: Zookeeper
- 8080: Kafka UI

**Máy Airflow:**
- 8080: Airflow Webserver

## Security Notes

- Thay đổi password mặc định của Airflow
- Sử dụng SSH keys thay vì passwords
- Cấu hình firewall phù hợp
- Sử dụng VPN hoặc private network cho production

