"""
Spark Streaming - Đọc dữ liệu từ Kafka, dự đoán và gửi kết quả về Kafka
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml import PipelineModel
import sys

def create_spark_session():
    """Tạo SparkSession với cấu hình cho streaming"""
    spark = SparkSession.builder \
        .appName("HeartFailureStreaming") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_model(model_path='models/heart_failure_model'):
    """Load mô hình đã huấn luyện"""
    print(f"Đang load mô hình từ {model_path}...")
    model = PipelineModel.load(model_path)
    print("Đã load mô hình thành công!")
    return model

def define_schema():
    """Định nghĩa schema cho dữ liệu từ Kafka"""
    schema = StructType([
        StructField("age", IntegerType(), True),
        StructField("anaemia", IntegerType(), True),
        StructField("creatinine_phosphokinase", IntegerType(), True),
        StructField("diabetes", IntegerType(), True),
        StructField("ejection_fraction", IntegerType(), True),
        StructField("high_blood_pressure", IntegerType(), True),
        StructField("platelets", DoubleType(), True),
        StructField("serum_creatinine", DoubleType(), True),
        StructField("serum_sodium", IntegerType(), True),
        StructField("sex", IntegerType(), True),
        StructField("smoking", IntegerType(), True),
        StructField("time", IntegerType(), True),
        StructField("DEATH_EVENT", IntegerType(), True)  # Có thể có hoặc không
    ])
    return schema

def process_streaming(spark, model, 
                     input_topic='heart_failure_input',
                     output_topic='heart_failure_predictions',
                     kafka_bootstrap_servers='localhost:9092',
                     checkpoint_location='checkpoints/streaming'):
    """
    Xử lý streaming: đọc từ Kafka, dự đoán, gửi kết quả về Kafka
    
    Args:
        spark: SparkSession
        model: PipelineModel đã load
        input_topic: Topic Kafka input
        output_topic: Topic Kafka output
        kafka_bootstrap_servers: Địa chỉ Kafka brokers
        checkpoint_location: Thư mục checkpoint cho Spark Streaming
    """
    print("=" * 50)
    print("BẮT ĐẦU SPARK STREAMING")
    print("=" * 50)
    
    print(f"\nInput topic: {input_topic}")
    print(f"Output topic: {output_topic}")
    print(f"Kafka servers: {kafka_bootstrap_servers}")
    
    # Đọc stream từ Kafka
    print("\nĐang kết nối đến Kafka...")
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", input_topic) \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON từ Kafka
    schema = define_schema()
    
    # Chuyển đổi value từ binary sang string rồi parse JSON
    df_parsed = df.select(
        col("key").cast("string").alias("record_id"),
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("record_id", "data.*")
    
    # Chuẩn bị features cho prediction
    feature_cols = [
        'age', 'anaemia', 'creatinine_phosphokinase', 'diabetes',
        'ejection_fraction', 'high_blood_pressure', 'platelets',
        'serum_creatinine', 'serum_sodium', 'sex', 'smoking'
    ]
    
    # Đảm bảo các cột có kiểu dữ liệu đúng
    for col_name in feature_cols:
        if col_name in ['age', 'anaemia', 'creatinine_phosphokinase', 'diabetes',
                       'ejection_fraction', 'high_blood_pressure', 'serum_sodium', 
                       'sex', 'smoking']:
            df_parsed = df_parsed.withColumn(col_name, col(col_name).cast("integer"))
        else:
            df_parsed = df_parsed.withColumn(col_name, col(col_name).cast("double"))
    
    # Dự đoán
    print("\nĐang thực hiện dự đoán...")
    predictions = model.transform(df_parsed)
    
    # Chuẩn bị kết quả để gửi về Kafka
    # Lấy probability của class 1 (death event)
    from pyspark.sql.functions import udf
    from pyspark.sql.types import DoubleType
    
    get_probability = udf(lambda v: float(v[1]), DoubleType())
    
    results = predictions.select(
        col("record_id").alias("key"),
        to_json(struct(
            col("record_id").alias("id"),
            col("age"),
            col("anaemia"),
            col("creatinine_phosphokinase"),
            col("diabetes"),
            col("ejection_fraction"),
            col("high_blood_pressure"),
            col("platelets"),
            col("serum_creatinine"),
            col("serum_sodium"),
            col("sex"),
            col("smoking"),
            col("time"),
            col("prediction").cast("integer").alias("predicted_death_event"),
            get_probability(col("probability")).alias("death_probability"),
            col("DEATH_EVENT").alias("actual_death_event")
        )).alias("value")
    )
    
    # Ghi kết quả về Kafka
    print(f"\nĐang gửi kết quả về topic {output_topic}...")
    query = results \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", output_topic) \
        .option("checkpointLocation", checkpoint_location) \
        .outputMode("append") \
        .start()
    
    print("\nSpark Streaming đang chạy...")
    print("Nhấn Ctrl+C để dừng")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n\nĐang dừng streaming...")
        query.stop()
        print("Đã dừng streaming")

if __name__ == "__main__":
    # Tham số từ dòng lệnh
    model_path = sys.argv[1] if len(sys.argv) > 1 else 'models/heart_failure_model'
    input_topic = sys.argv[2] if len(sys.argv) > 2 else 'heart_failure_input'
    output_topic = sys.argv[3] if len(sys.argv) > 3 else 'heart_failure_predictions'
    kafka_servers = sys.argv[4] if len(sys.argv) > 4 else 'localhost:9092'
    
    spark = create_spark_session()
    
    try:
        model = load_model(model_path)
        process_streaming(spark, model, input_topic, output_topic, kafka_servers)
    finally:
        spark.stop()

