#!/usr/bin/env python3
"""
Consumer để đọc kết quả dự đoán từ Kafka và trực quan hóa
"""

import json
import pandas as pd
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from collections import deque
import sys
import threading
import time

class PredictionVisualizer:
    def __init__(self, kafka_topic='heart_failure_predictions', 
                 bootstrap_servers='192.168.80.157:9092',
                 max_points=50):
        self.kafka_topic = kafka_topic
        self.bootstrap_servers = bootstrap_servers
        self.max_points = max_points
        
        # Dữ liệu để vẽ biểu đồ
        self.timestamps = deque(maxlen=max_points)
        self.predictions = deque(maxlen=max_points)
        self.probabilities = deque(maxlen=max_points)
        self.actuals = deque(maxlen=max_points)
        self.data_lock = threading.Lock()  # Lock để thread-safe
        
        # Tạo consumer với cấu hình tốt hơn
        try:
            print(f"Đang kết nối đến Kafka tại {bootstrap_servers}...")
            self.consumer = KafkaConsumer(
                kafka_topic,
                bootstrap_servers=bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',  # Chỉ đọc message mới
                enable_auto_commit=True,
                group_id='visualization_consumer_group',  # Group ID để quản lý offset
                api_version=(0, 10, 1)  # Đảm bảo tương thích với Kafka
            )
            # Kiểm tra kết nối bằng cách lấy metadata
            partitions = self.consumer.partitions_for_topic(kafka_topic)
            if partitions:
                print(f"✓ Đã kết nối thành công! Topic '{kafka_topic}' có {len(partitions)} partition(s)")
            else:
                print(f"⚠ Cảnh báo: Không tìm thấy partition cho topic '{kafka_topic}'")
                print(f"  Có thể topic chưa được tạo hoặc chưa có dữ liệu")
        except Exception as e:
            print(f"✗ Lỗi kết nối đến Kafka: {e}")
            print(f"  Kiểm tra:")
            print(f"  - Kafka đang chạy tại {bootstrap_servers}")
            print(f"  - Topic '{kafka_topic}' đã được tạo")
            print(f"  - Kết nối mạng đến Kafka server")
            raise
        
        # Thiết lập matplotlib với backend không blocking
        plt.ion()  # Bật interactive mode
        self.fig, self.axes = plt.subplots(2, 1, figsize=(12, 8))
        self.fig.suptitle('Heart Failure Prediction - Real-time Visualization', fontsize=14)
        
        # Thread để đọc message từ Kafka
        self.running = True
        self.consumer_thread = None
        
    def consume_messages(self):
        """Thread function để đọc message từ Kafka"""
        print("Bắt đầu đọc message từ Kafka...")
        print("Đang chờ message mới...")
        message_count = 0
        last_message_time = time.time()
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                    
                try:
                    data = message.value
                    last_message_time = time.time()
                    
                    # Thêm dữ liệu vào deque với lock
                    with self.data_lock:
                        timestamp = len(self.timestamps)
                        self.timestamps.append(timestamp)
                        self.predictions.append(data.get('predicted_death_event', 0))
                        self.probabilities.append(data.get('death_probability', 0.0))
                        self.actuals.append(data.get('actual_death_event', None))
                    
                    message_count += 1
                    print(f"\n[{message_count}] Record {data.get('id', 'N/A')}: "
                          f"Predicted={data.get('predicted_death_event')}, "
                          f"Probability={data.get('death_probability', 0):.3f}, "
                          f"Actual={data.get('actual_death_event', 'N/A')}")
                    
                except json.JSONDecodeError as e:
                    print(f"Lỗi decode JSON: {e}")
                except Exception as e:
                    print(f"Lỗi khi xử lý message: {e}")
                    import traceback
                    traceback.print_exc()
                    
        except KeyboardInterrupt:
            print("\nConsumer thread nhận tín hiệu dừng")
        except Exception as e:
            if self.running:
                print(f"Lỗi khi đọc từ Kafka: {e}")
                import traceback
                traceback.print_exc()
        
        print("Đã dừng đọc message từ Kafka")
        
    def update_plot(self, frame):
        """Cập nhật biểu đồ với dữ liệu mới"""
        try:
            # Lấy dữ liệu với lock
            with self.data_lock:
                timestamps = list(self.timestamps)
                predictions = list(self.predictions)
                probabilities = list(self.probabilities)
                actuals = list(self.actuals)
            
            # Vẽ biểu đồ 1: Predictions over time
            self.axes[0].clear()
            if len(timestamps) > 0:
                self.axes[0].plot(timestamps, predictions, 
                                 'b-o', label='Predicted', markersize=8, linewidth=2)
                if any(a is not None for a in actuals):
                    actuals_list = [a if a is not None else 0 for a in actuals]
                    self.axes[0].plot(timestamps, actuals_list, 
                                     'r-s', label='Actual', markersize=8, alpha=0.7, linewidth=2)
                self.axes[0].set_ylabel('Death Event (0/1)', fontsize=10)
                self.axes[0].set_title(f'Predicted vs Actual Death Events (Total: {len(timestamps)})', fontsize=12)
                self.axes[0].legend(loc='upper right')
                self.axes[0].grid(True, alpha=0.3)
                self.axes[0].set_ylim(-0.1, 1.1)
                if len(timestamps) > 1:
                    self.axes[0].set_xlim(min(timestamps) - 1, max(timestamps) + 1)
            
            # Vẽ biểu đồ 2: Probability over time
            self.axes[1].clear()
            if len(timestamps) > 0:
                self.axes[1].plot(timestamps, probabilities, 
                                 'g-o', label='Death Probability', markersize=8, linewidth=2)
                self.axes[1].axhline(y=0.5, color='r', linestyle='--', 
                                    label='Threshold (0.5)', alpha=0.5, linewidth=2)
                self.axes[1].set_xlabel('Record Number', fontsize=10)
                self.axes[1].set_ylabel('Probability', fontsize=10)
                self.axes[1].set_title('Death Event Probability Over Time', fontsize=12)
                self.axes[1].legend(loc='upper right')
                self.axes[1].grid(True, alpha=0.3)
                self.axes[1].set_ylim(-0.05, 1.05)
                if len(timestamps) > 1:
                    self.axes[1].set_xlim(min(timestamps) - 1, max(timestamps) + 1)
            
            plt.tight_layout()
            self.fig.canvas.draw()
            self.fig.canvas.flush_events()
            
        except Exception as e:
            print(f"Lỗi khi cập nhật biểu đồ: {e}")
    
    def start(self):
        """Bắt đầu visualization"""
        print("=" * 50)
        print("BẮT ĐẦU TRỰC QUAN HÓA DỮ LIỆU")
        print("=" * 50)
        print(f"Đang đọc từ topic: {self.kafka_topic}")
        print(f"Kafka server: {self.bootstrap_servers}")
        print("Nhấn Ctrl+C để dừng")
        print("=" * 50)
        
        # Khởi động thread để đọc message
        self.consumer_thread = threading.Thread(target=self.consume_messages, daemon=True)
        self.consumer_thread.start()
        
        # Đợi một chút để consumer kết nối
        time.sleep(2)
        
        # Tạo animation
        ani = animation.FuncAnimation(self.fig, self.update_plot, interval=500, blit=False)
        
        try:
            plt.show(block=True)
        except KeyboardInterrupt:
            print("\n\nĐang dừng visualization...")
            self.running = False
            if self.consumer_thread:
                self.consumer_thread.join(timeout=2)
            self.consumer.close()
            plt.close('all')
            print("Đã dừng")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Visualization consumer cho Heart Failure predictions')
    parser.add_argument('--kafka-topic', default='heart_failure_predictions',
                       help='Topic Kafka để đọc kết quả')
    parser.add_argument('--bootstrap-servers', default='192.168.80.157:9092',
                       help='Địa chỉ Kafka brokers')
    parser.add_argument('--max-points', type=int, default=50,
                       help='Số điểm tối đa hiển thị trên biểu đồ')
    
    args = parser.parse_args()
    
    visualizer = PredictionVisualizer(
        kafka_topic=args.kafka_topic,
        bootstrap_servers=args.bootstrap_servers,
        max_points=args.max_points
    )
    
    visualizer.start()

