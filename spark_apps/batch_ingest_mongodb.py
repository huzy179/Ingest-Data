import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date

def create_spark_session():
    # Khởi tạo Spark Session với cấu hình S3A
    spark = SparkSession.builder \
        .appName("Batch-Ingest-MongoDB") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio_admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio_password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    return spark

def ingest_collection(spark, collection_name):
    print(f"Start batch ingesting collection {collection_name} from MongoDB to MinIO...")
    
    # Kết nối MongoDB bằng Spark MongoDB Connector
    mongo_uri = "mongodb://admin:admin@mongo:27017/?authSource=admin"
    
    df = spark.read \
        .format("mongodb") \
        .option("connection.uri", mongo_uri) \
        .option("database", "banking_events") \
        .option("collection", collection_name) \
        .load()
        
    # Thêm cột load_date để làm phân vùng bất biến (Immutable Partitioning)
    df_partitioned = df.withColumn("load_date", current_date().cast("string"))
        
    # Đường dẫn ghi trên MinIO
    output_path = f"s3a://banking-lakehouse/batch/mongodb/{collection_name}"
    
    # Ghi dữ liệu dạng Parquet vào Bronze layer phân vùng theo load_date để tối ưu dung lượng và tốc độ quét
    df_partitioned.write \
        .format("parquet") \
        .partitionBy("load_date") \
        .mode("append") \
        .save(output_path)
        
    print(f"Successfully ingested {df.count()} rows from {collection_name} to {output_path}")

def main():
    spark = create_spark_session()
    try:
        collections = [
            "customers",
            "login_events",
            "fraud_events",
            "audit_logs",
            "notification_logs",
            "device_events"
        ]
        for col_name in collections:
            try:
                ingest_collection(spark, col_name)
            except Exception as e:
                print(f"Error ingesting collection {col_name}: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
