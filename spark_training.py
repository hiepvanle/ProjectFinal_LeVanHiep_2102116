"""
Spark ML - Huấn luyện mô hình dự đoán Heart Failure
Sử dụng Random Forest Classifier
"""

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import sys

def train_model(spark, train_data_path='data/train_data.csv', model_path='models/heart_failure_model'):
    """
    Huấn luyện mô hình Random Forest để dự đoán Heart Failure
    
    Args:
        spark: SparkSession
        train_data_path: Đường dẫn đến file dữ liệu huấn luyện
        model_path: Đường dẫn lưu mô hình
    """
    print("=" * 50)
    print("BẮT ĐẦU HUẤN LUYỆN MÔ HÌNH")
    print("=" * 50)
    
    # Đọc dữ liệu huấn luyện
    print(f"\nĐang đọc dữ liệu từ {train_data_path}...")
    df = spark.read.csv(train_data_path, header=True, inferSchema=True)
    
    print(f"Số dòng dữ liệu: {df.count()}")
    print("\nSchema của dữ liệu:")
    df.printSchema()
    
    # Các feature columns (loại bỏ DEATH_EVENT và time)
    feature_cols = [
        'age', 'anaemia', 'creatinine_phosphokinase', 'diabetes',
        'ejection_fraction', 'high_blood_pressure', 'platelets',
        'serum_creatinine', 'serum_sodium', 'sex', 'smoking'
    ]
    
    # Tạo VectorAssembler để kết hợp các features
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol='features',
        handleInvalid='skip'
    )
    
    # Tạo label indexer (DEATH_EVENT đã là 0/1 nên không cần index)
    # Nhưng để đảm bảo, ta sẽ đổi tên thành label
    from pyspark.sql.functions import col
    df = df.withColumnRenamed('DEATH_EVENT', 'label')
    
    # Tạo pipeline
    rf = RandomForestClassifier(
        labelCol='label',
        featuresCol='features',
        numTrees=100,
        maxDepth=10,
        seed=42
    )
    
    pipeline = Pipeline(stages=[assembler, rf])
    
    # Huấn luyện mô hình
    print("\nĐang huấn luyện mô hình Random Forest...")
    model = pipeline.fit(df)
    
    # Đánh giá mô hình trên tập huấn luyện
    print("\nĐang đánh giá mô hình...")
    predictions = model.transform(df)
    
    evaluator = BinaryClassificationEvaluator(
        labelCol='label',
        rawPredictionCol='rawPrediction'
    )
    
    auc = evaluator.evaluate(predictions)
    print(f"\nAUC Score: {auc:.4f}")
    
    # Hiển thị một số dự đoán mẫu
    print("\nMột số dự đoán mẫu:")
    predictions.select('label', 'prediction', 'probability').show(10)
    
    # Lưu mô hình
    print(f"\nĐang lưu mô hình vào {model_path}...")
    model.write().overwrite().save(model_path)
    print("Đã lưu mô hình thành công!")
    
    print("\n" + "=" * 50)
    print("HOÀN THÀNH HUẤN LUYỆN MÔ HÌNH")
    print("=" * 50)
    
    return model

if __name__ == "__main__":
    # Tạo SparkSession
    spark = SparkSession.builder \
        .appName("HeartFailureTraining") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()
    
    # Đường dẫn mặc định hoặc từ tham số dòng lệnh
    train_data_path = sys.argv[1] if len(sys.argv) > 1 else 'data/train_data.csv'
    model_path = sys.argv[2] if len(sys.argv) > 2 else 'models/heart_failure_model'
    
    try:
        model = train_model(spark, train_data_path, model_path)
    finally:
        spark.stop()

