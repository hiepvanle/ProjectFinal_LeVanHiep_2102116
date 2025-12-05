#!/usr/bin/env python3
"""
Script để chia dữ liệu thành 2 phần:
- Phần 1: Dùng để huấn luyện (train_data.csv)
- Phần 2: Dùng để streaming về Kafka (stream_data.csv)
"""

import pandas as pd
from sklearn.model_selection import train_test_split

def split_data(input_file='Heart_failure_clinical_records_dataset.csv', 
               train_file='data/train_data.csv',
               stream_file='data/stream_data.csv',
               test_size=0.3,
               random_state=42):
    """
    Chia dữ liệu thành tập huấn luyện và tập streaming
    
    Args:
        input_file: File dữ liệu gốc
        train_file: File lưu dữ liệu huấn luyện
        stream_file: File lưu dữ liệu streaming
        test_size: Tỷ lệ dữ liệu dùng cho streaming (mặc định 30%)
        random_state: Seed cho random
    """
    print(f"Đang đọc dữ liệu từ {input_file}...")
    df = pd.read_csv(input_file)
    
    print(f"Tổng số dòng dữ liệu: {len(df)}")
    
    # Chia dữ liệu: 70% train, 30% stream
    train_df, stream_df = train_test_split(
        df, 
        test_size=test_size, 
        random_state=random_state,
        stratify=df['DEATH_EVENT']  # Giữ tỷ lệ phân bố target
    )
    
    print(f"Số dòng dữ liệu huấn luyện: {len(train_df)}")
    print(f"Số dòng dữ liệu streaming: {len(stream_df)}")
    
    # Lưu dữ liệu
    train_df.to_csv(train_file, index=False)
    stream_df.to_csv(stream_file, index=False)
    
    print(f"Đã lưu dữ liệu huấn luyện vào {train_file}")
    print(f"Đã lưu dữ liệu streaming vào {stream_file}")
    
    return train_df, stream_df

if __name__ == "__main__":
    import os
    
    # Tạo thư mục data nếu chưa có
    os.makedirs('data', exist_ok=True)
    
    # Chia dữ liệu
    train_df, stream_df = split_data()
    
    print("\nHoàn thành chia dữ liệu!")

