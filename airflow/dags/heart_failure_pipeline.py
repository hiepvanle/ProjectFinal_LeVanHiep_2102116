"""
Airflow DAG để điều khiển toàn bộ quá trình:
1. Khởi động Kafka (Docker)
2. Chạy Spark server (nếu cần)
3. Chia dữ liệu
4. Huấn luyện mô hình Spark ML
5. Chạy chương trình mô phỏng streaming
6. Submit code Spark Streaming để xử lý và dự đoán
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import time

# Đường dẫn dự án
PROJECT_DIR = "/home/labsit28/Downloads/PROJECTFINAL_LEVANHIEP_2102116"
SPARK_HOME = os.environ.get('SPARK_HOME', '/opt/spark')
KAFKA_SERVERS = os.environ.get('KAFKA_SERVERS', '192.168.80.88:8080')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'heart_failure_pipeline',
    default_args=default_args,
    description='Heart Failure Prediction Pipeline với Spark ML và Kafka',
    schedule_interval=None,  # Chạy thủ công
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'kafka', 'ml', 'streaming'],
)

def wait_for_kafka(**context):
    """Đợi Kafka sẵn sàng"""
    import socket
    import time
    
    host, port = KAFKA_SERVERS.split(':')
    max_retries = 30
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex((host, int(port)))
            sock.close()
            if result == 0:
                print(f"Kafka đã sẵn sàng tại {KAFKA_SERVERS}")
                return
        except Exception as e:
            print(f"Đang đợi Kafka... ({retry_count}/{max_retries})")
        
        retry_count += 1
        time.sleep(2)
    
    raise Exception(f"Kafka không sẵn sàng sau {max_retries} lần thử")

def create_kafka_topics(**context):
    """Tạo các Kafka topics cần thiết"""
    import subprocess
    
    topics = ['heart_failure_input', 'heart_failure_predictions']
    
    for topic in topics:
        cmd = f"docker exec kafka kafka-topics --create --topic {topic} --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists"
        try:
            subprocess.run(cmd, shell=True, check=True, cwd=PROJECT_DIR)
            print(f"Đã tạo topic: {topic}")
        except subprocess.CalledProcessError as e:
            print(f"Topic {topic} có thể đã tồn tại hoặc lỗi: {e}")

# Task 1: Khởi động Kafka (Docker)
start_kafka = BashOperator(
    task_id='start_kafka',
    bash_command=f'cd {PROJECT_DIR} && docker-compose up -d',
    dag=dag,
)

# Task 2: Đợi Kafka sẵn sàng
wait_kafka = PythonOperator(
    task_id='wait_for_kafka',
    python_callable=wait_for_kafka,
    dag=dag,
)

# Task 3: Tạo Kafka topics
create_topics = PythonOperator(
    task_id='create_kafka_topics',
    python_callable=create_kafka_topics,
    dag=dag,
)

# Task 4: Chia dữ liệu
split_data_task = BashOperator(
    task_id='split_data',
    bash_command=f'cd {PROJECT_DIR} && python3 data_split.py',
    dag=dag,
)

# Task 5: Huấn luyện mô hình Spark ML
train_model_task = BashOperator(
    task_id='train_model',
    bash_command=f'cd {PROJECT_DIR} && {SPARK_HOME}/bin/spark-submit '
                 f'--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 '
                 f'spark_training.py data/train_data.csv models/heart_failure_model',
    dag=dag,
)

# Task 6: Chạy Spark Streaming (chạy trong background)
start_spark_streaming = BashOperator(
    task_id='start_spark_streaming',
    bash_command=f'cd {PROJECT_DIR} && nohup {SPARK_HOME}/bin/spark-submit '
                 f'--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 '
                 f'spark_streaming.py models/heart_failure_model heart_failure_input heart_failure_predictions {KAFKA_SERVERS} '
                 f'> logs/spark_streaming.log 2>&1 & echo $! > /tmp/spark_streaming.pid',
    dag=dag,
)

# Task 7: Đợi một chút để Spark Streaming khởi động
wait_spark_streaming = BashOperator(
    task_id='wait_spark_streaming',
    bash_command='sleep 10',
    dag=dag,
)

# Task 8: Chạy chương trình mô phỏng streaming
run_stream_simulator = BashOperator(
    task_id='run_stream_simulator',
    bash_command=f'cd {PROJECT_DIR} && python3 stream_simulator.py '
                 f'--stream-file data/stream_data.csv '
                 f'--kafka-topic heart_failure_input '
                 f'--bootstrap-servers {KAFKA_SERVERS} '
                 f'--delay 2',
    dag=dag,
)

# Định nghĩa thứ tự các task
start_kafka >> wait_kafka >> create_topics >> split_data_task >> train_model_task >> start_spark_streaming >> wait_spark_streaming >> run_stream_simulator

