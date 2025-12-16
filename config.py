"""
File cấu hình cho toàn bộ hệ thống
Cập nhật các giá trị này theo môi trường của bạn
"""

# Đường dẫn dự án
PROJECT_DIR = "/home/labsit28/Downloads/PROJECTFINAL_LEVANHIEP_2102116"

# Spark Configuration
SPARK_HOME = "/opt/spark"  # Cập nhật đường dẫn Spark của bạn
SPARK_MASTER = "spark://192.168.80.157:7077"  # Hoặc "local[*]" cho local

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = "192.168.80.157:9092"  # Cập nhật IP máy Kafka
KAFKA_INPUT_TOPIC = "heart_failure_input"
KAFKA_OUTPUT_TOPIC = "heart_failure_predictions"

# Data paths
TRAIN_DATA_PATH = "data/train_data.csv"
STREAM_DATA_PATH = "data/stream_data.csv"
MODEL_PATH = "models/heart_failure_model"

# Streaming Configuration
STREAM_DELAY_SECONDS = 2.0  # Delay giữa các records (giây)

# Airflow Configuration
AIRFLOW_DAG_ID = "heart_failure_pipeline"

# Machine IPs (cập nhật theo môi trường của bạn)
SPARK_SERVER_IP = "192.168.80.141"  # IP máy Spark
KAFKA_SERVER_IP = "192.168.80.157"  # IP máy Kafka
AIRFLOW_SERVER_IP = "192.168.80.130"  # IP máy Airflow

