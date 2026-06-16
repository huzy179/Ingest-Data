import os
import sys
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

def create_spark_session():
    # Khởi tạo Spark Session có tích hợp Delta Lake Extension và Catalog
    spark = SparkSession.builder \
        .appName("Delta-Lake-Maintenance") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio_admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio_password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark

def maintain_delta_tables():
    spark = create_spark_session()
    
    tables = [
        "silver_postgres_customers",
        "silver_postgres_cards",
        "silver_postgres_transactions",
        "silver_mongo_login_events",
        "silver_mongo_device_events",
        "silver_mongo_fraud_events",
        "silver_mongo_notification_logs",
        "silver_mongo_audit_logs"
    ]
    
    print("Starting Delta Lake maintenance (OPTIMIZE & VACUUM)...")
    
    for table_name in tables:
        delta_path = f"s3a://banking-lakehouse/silver/delta/{table_name}"
        print(f"\nProcessing maintenance for table: {table_name} at {delta_path}")
        
        try:
            if DeltaTable.isDeltaTable(spark, delta_path):
                delta_table = DeltaTable.forPath(spark, delta_path)
                
                # 1. OPTIMIZE: Gom các file nhỏ thành file to (~1GB)
                print(f"Running OPTIMIZE on {table_name}...")
                optimize_metrics = delta_table.optimize().executeCompaction()
                print(f"OPTIMIZE finished successfully for {table_name}.")
                
                # 2. VACUUM: Xóa các file rác cũ không còn dùng (giữ lại log 7 ngày = 168 giờ)
                # Lưu ý: Cần tắt spark.databricks.delta.vacuum.parallelDelete.enabled hoặc cấu hình an toàn nếu cần,
                # nhưng vacuum mặc định giữ 168 giờ (7 ngày) là an toàn mà không cần tắt kiểm tra thời gian lưu trữ.
                print(f"Running VACUUM (retain 168 hours) on {table_name}...")
                vacuum_df = delta_table.vacuum(168.0)
                print(f"VACUUM finished successfully for {table_name}.")
                
            else:
                print(f"Path {delta_path} is not a valid Delta table. Skipping.")
        except Exception as e:
            print(f"Error maintaining table {table_name}: {e}", file=sys.stderr)
            
    spark.stop()
    print("\nDelta Lake maintenance completed!")

if __name__ == "__main__":
    maintain_delta_tables()
