import os
from pyspark.sql import SparkSession

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
    # Sử dụng connection.uri cho bản 10.x trở lên
    mongo_uri = "mongodb://admin:admin@mongo:27017/?authSource=admin"
    
    df = spark.read \
        .format("mongodb") \
        .option("connection.uri", mongo_uri) \
        .option("database", "banking_events") \
        .option("collection", collection_name) \
        .load()
        
    # Đường dẫn ghi trên MinIO
    output_path = f"s3a://banking-lakehouse/batch/mongodb/{collection_name}"
    
    # Ghi dữ liệu dạng JSON vào Bronze layer
    df.write \
        .format("json") \
        .mode("overwrite") \
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
