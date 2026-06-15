import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date

def create_spark_session():
    # Khởi tạo Spark Session với cấu hình AWS S3A để ghi dữ liệu vào MinIO
    spark = SparkSession.builder \
        .appName("Batch-Ingest-PostgreSQL") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio_admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio_password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    return spark

def ingest_table(spark, table_name):
    print(f"Start batch ingesting table {table_name} from PostgreSQL to MinIO...")
    
    # Kết nối JDBC đến PostgreSQL service trong Docker network
    jdbc_url = "jdbc:postgresql://postgres:5432/banking_core"
    
    # Đọc dữ liệu từ Postgres
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .load()
        
    # Thêm cột load_date để làm phân vùng bất biến (Immutable Partitioning)
    df_partitioned = df.withColumn("load_date", current_date().cast("string"))
        
    # Đường dẫn ghi trên MinIO
    output_path = f"s3a://banking-lakehouse/batch/postgres/{table_name}"
    
    # Ghi dữ liệu dạng Parquet vào Bronze layer phân vùng theo load_date để tối ưu dung lượng và tốc độ quét
    df_partitioned.write \
        .format("parquet") \
        .partitionBy("load_date") \
        .mode("append") \
        .save(output_path)
        
    print(f"Successfully ingested {df.count()} rows from {table_name} to {output_path}")

def main():
    spark = create_spark_session()
    try:
        tables = ["customers", "cards", "transactions"]
        for table in tables:
            ingest_table(spark, table)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
