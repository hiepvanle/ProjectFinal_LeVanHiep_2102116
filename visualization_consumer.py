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

class PredictionVisualizer:
    def __init__(self, kafka_topic='heart_failure_predictions', 
                 bootstrap_servers='192.168.80.88:9092',
                 max_points=50):
        self.kafka_topic = kafka_topic
        self.bootstrap_servers = bootstrap_servers
        self.max_points = max_points
        
        # Dữ liệu để vẽ biểu đồ
        self.timestamps = deque(maxlen=max_points)
        self.predictions = deque(maxlen=max_points)
        self.probabilities = deque(maxlen=max_points)
        self.actuals = deque(maxlen=max_points)
        
        # Tạo consumer
        self.consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            consumer_timeout_ms=1000
        )
        
        # Thiết lập matplotlib
        self.fig, self.axes = plt.subplots(2, 1, figsize=(12, 8))
        self.fig.suptitle('Heart Failure Prediction - Real-time Visualization', fontsize=14)
        
    def update_plot(self, frame):
        """Cập nhật biểu đồ với dữ liệu mới"""
        try:
            # Đọc message từ Kafka
            message = self.consumer.poll(timeout_ms=100)
            
            if message:
                for topic_partition, messages in message.items():
                    for msg in messages:
                        data = msg.value
                        
                        # Thêm dữ liệu vào deque
                        timestamp = len(self.timestamps)
                        self.timestamps.append(timestamp)
                        self.predictions.append(data.get('predicted_death_event', 0))
                        self.probabilities.append(data.get('death_probability', 0.0))
                        self.actuals.append(data.get('actual_death_event', None))
                        
                        print(f"Record {data.get('id', 'N/A')}: "
                              f"Predicted={data.get('predicted_death_event')}, "
                              f"Probability={data.get('death_probability', 0):.3f}, "
                              f"Actual={data.get('actual_death_event', 'N/A')}")
            
            # Vẽ biểu đồ 1: Predictions over time
            self.axes[0].clear()
            if len(self.timestamps) > 0:
                self.axes[0].plot(list(self.timestamps), list(self.predictions), 
                                 'b-o', label='Predicted', markersize=8)
                if any(a is not None for a in self.actuals):
                    actuals_list = [a if a is not None else 0 for a in self.actuals]
                    self.axes[0].plot(list(self.timestamps), actuals_list, 
                                     'r-s', label='Actual', markersize=8, alpha=0.7)
                self.axes[0].set_ylabel('Death Event (0/1)')
                self.axes[0].set_title('Predicted vs Actual Death Events')
                self.axes[0].legend()
                self.axes[0].grid(True, alpha=0.3)
                self.axes[0].set_ylim(-0.1, 1.1)
            
            # Vẽ biểu đồ 2: Probability over time
            self.axes[1].clear()
            if len(self.timestamps) > 0:
                self.axes[1].plot(list(self.timestamps), list(self.probabilities), 
                                 'g-o', label='Death Probability', markersize=8)
                self.axes[1].axhline(y=0.5, color='r', linestyle='--', 
                                    label='Threshold (0.5)', alpha=0.5)
                self.axes[1].set_xlabel('Record Number')
                self.axes[1].set_ylabel('Probability')
                self.axes[1].set_title('Death Event Probability Over Time')
                self.axes[1].legend()
                self.axes[1].grid(True, alpha=0.3)
                self.axes[1].set_ylim(-0.05, 1.05)
            
            plt.tight_layout()
            
        except Exception as e:
            print(f"Lỗi khi cập nhật biểu đồ: {e}")
    
    def start(self):
        """Bắt đầu visualization"""
        print("=" * 50)
        print("BẮT ĐẦU TRỰC QUAN HÓA DỮ LIỆU")
        print("=" * 50)
        print(f"Đang đọc từ topic: {self.kafka_topic}")
        print("Nhấn Ctrl+C để dừng")
        
        # Tạo animation
        ani = animation.FuncAnimation(self.fig, self.update_plot, interval=1000, blit=False)
        
        try:
            plt.show()
        except KeyboardInterrupt:
            print("\n\nĐang dừng visualization...")
            self.consumer.close()
            print("Đã dừng")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Visualization consumer cho Heart Failure predictions')
    parser.add_argument('--kafka-topic', default='heart_failure_predictions',
                       help='Topic Kafka để đọc kết quả')
    parser.add_argument('--bootstrap-servers', default='192.168.80.88:9092',
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

