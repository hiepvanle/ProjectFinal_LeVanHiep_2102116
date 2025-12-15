#!/usr/bin/env python3
"""
Chương trình mô phỏng streaming để gửi dữ liệu đến Kafka
Đọc từ file stream_data.csv và gửi từng dòng với tốc độ định kỳ
"""

import time
import json
import pandas as pd
from kafka import KafkaProducer
import sys
import os

def create_producer(bootstrap_servers='192.168.80.131:9092'):
    """Tạo Kafka Producer"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        print(f"Đã kết nối Kafka tại {bootstrap_servers}")
        return producer
    except Exception as e:
        print(f"Lỗi kết nối Kafka: {e}")
        raise

def send_streaming_data(stream_file='data/stream_data.csv', 
                       kafka_topic='heart_failure_input',
                       bootstrap_servers='192.168.80.131:9092',
                       delay_seconds=2):
    """
    Gửi dữ liệu streaming đến Kafka
    
    Args:
        stream_file: File chứa dữ liệu streaming
        kafka_topic: Topic Kafka để gửi dữ liệu
        bootstrap_servers: Địa chỉ Kafka brokers
        delay_seconds: Thời gian delay giữa các dòng (giây)
    """
    print("=" * 50)
    print("BẮT ĐẦU STREAMING DỮ LIỆU ĐẾN KAFKA")
    print("=" * 50)
    
    # Đọc dữ liệu streaming
    if not os.path.exists(stream_file):
        print(f"Lỗi: Không tìm thấy file {stream_file}")
        return
    
    print(f"\nĐang đọc dữ liệu từ {stream_file}...")
    df = pd.read_csv(stream_file)
    
    print(f"Tổng số dòng dữ liệu: {len(df)}")
    print(f"Topic Kafka: {kafka_topic}")
    print(f"Delay giữa các dòng: {delay_seconds} giây")
    
    # Tạo producer
    producer = create_producer(bootstrap_servers)
    
    # Gửi từng dòng dữ liệu
    sent_count = 0
    try:
        for idx, row in df.iterrows():
            # Chuyển đổi row thành dictionary
            record = row.to_dict()
            
            # Loại bỏ DEATH_EVENT khỏi dữ liệu gửi đi (vì đây là giá trị cần dự đoán)
            # Nhưng có thể giữ lại để so sánh sau
            record_id = f"record_{idx}"
            
            # Gửi đến Kafka
            future = producer.send(
                kafka_topic,
                key=record_id,
                value=record
            )
            
            # Đợi gửi thành công
            try:
                record_metadata = future.get(timeout=10)
                sent_count += 1
                print(f"[{sent_count}] Đã gửi record {record_id} đến topic {record_metadata.topic}, "
                      f"partition {record_metadata.partition}, offset {record_metadata.offset}")
            except Exception as e:
                print(f"Lỗi khi gửi record {record_id}: {e}")
            
            # Delay trước khi gửi dòng tiếp theo
            time.sleep(delay_seconds)
            
    except KeyboardInterrupt:
        print("\n\nĐã dừng streaming do người dùng yêu cầu")
    except Exception as e:
        print(f"\nLỗi khi streaming: {e}")
    finally:
        producer.flush()
        producer.close()
        print(f"\nĐã gửi tổng cộng {sent_count} records")
        print("=" * 50)
        print("KẾT THÚC STREAMING")
        print("=" * 50)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Streaming simulator cho Heart Failure data')
    parser.add_argument('--stream-file', default='data/stream_data.csv',
                       help='File chứa dữ liệu streaming')
    parser.add_argument('--kafka-topic', default='heart_failure_input',
                       help='Topic Kafka để gửi dữ liệu')
    parser.add_argument('--bootstrap-servers', default='192.168.80.131:9092',
                       help='Địa chỉ Kafka brokers')
    parser.add_argument('--delay', type=float, default=2.0,
                       help='Delay giữa các dòng (giây)')
    
    args = parser.parse_args()
    
    send_streaming_data(
        stream_file=args.stream_file,
        kafka_topic=args.kafka_topic,
        bootstrap_servers=args.bootstrap_servers,
        delay_seconds=args.delay
    )

