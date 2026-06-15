import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, to_timestamp, when
from pyspark.sql.window import Window

def create_spark_session():
    # Khởi tạo Spark Session có tích hợp Delta Lake Extension và Catalog
    spark = SparkSession.builder \
        .appName("Data-Lakehouse-Silver-Transformation") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio_admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio_password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark

def get_input_path(topic, execution_date):
    if not execution_date:
        return f"s3a://banking-lakehouse/topics/{topic}/year=*/month=*/day=*/*.parquet"
    try:
        from datetime import datetime
        dt = datetime.strptime(execution_date, "%Y-%m-%d")
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        return f"s3a://banking-lakehouse/topics/{topic}/year={year}/month={month}/day={day}/*.parquet"
    except Exception as e:
        print(f"Error parsing date {execution_date}: {e}. Falling back to wildcard path.")
        return f"s3a://banking-lakehouse/topics/{topic}/year=*/month=*/day=*/*.parquet"

def write_to_clickhouse_and_delta(df, table_name):
    # Ghi dữ liệu vào Delta Lake trên MinIO (Silver Layer Delta)
    delta_path = f"s3a://banking-lakehouse/silver/delta/{table_name}"
    try:
        df.write \
            .format("delta") \
            .mode("append") \
            .save(delta_path)
        print(f"Successfully wrote data to Delta Lake: {delta_path}")
    except Exception as e:
        print(f"Error writing to Delta Lake for {table_name}: {e}")

    # Điền chuỗi rỗng cho các cột string bị null để tránh lỗi nullability của ClickHouse
    df_filled = df.na.fill("")
    
    # Tự động chuyển đổi các cột struct hoặc array thành JSON string để ClickHouse JDBC nhận dạng được
    from pyspark.sql.types import StructType, ArrayType
    from pyspark.sql.functions import to_json
    for field in df_filled.schema.fields:
        if isinstance(field.dataType, (StructType, ArrayType)):
            df_filled = df_filled.withColumn(field.name, to_json(col(field.name)))
            
    jdbc_url = "jdbc:clickhouse://clickhouse:8123/analytics"
    
    df_filled.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", "default") \
        .option("password", "admin") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .mode("append") \
        .save()
    print(f"Successfully wrote {df_filled.count()} rows to ClickHouse table: analytics.{table_name}")

def transform_postgres_customers(spark, execution_date=None):
    print("Transforming Postgres Customers...")
    path = get_input_path("postgres.public.customers", execution_date)
    try:
        df = spark.read.parquet(path)
    except Exception as e:
        if "PATH_NOT_FOUND" in str(e) or "Path does not exist" in str(e):
            print(f"No raw data found on S3 for postgres.public.customers yet. Skipping.")
            return
        raise e
    
    # De-duplicate: Lấy bản ghi mới nhất theo created_at
    window_spec = Window.partitionBy("id").orderBy(col("created_at").desc())
    df_dedup = df.withColumn("row_num", row_number().over(window_spec)) \
                 .filter(col("row_num") == 1) \
                 .drop("row_num")
                 
    # Ép kiểu dữ liệu
    df_cleaned = df_dedup \
        .withColumn("id", col("id").cast("long")) \
        .withColumn("current_age", col("current_age").cast("integer")) \
        .withColumn("retirement_age", col("retirement_age").cast("integer")) \
        .withColumn("birth_year", col("birth_year").cast("integer")) \
        .withColumn("birth_month", col("birth_month").cast("integer")) \
        .withColumn("fico_score", col("fico_score").cast("integer")) \
        .withColumn("num_credit_cards", col("num_credit_cards").cast("integer")) \
        .withColumn("latitude", col("latitude").cast("double")) \
        .withColumn("longitude", col("longitude").cast("double")) \
        .withColumn("per_capita_income_zipcode", col("per_capita_income_zipcode").cast("double")) \
        .withColumn("yearly_income", col("yearly_income").cast("double")) \
        .withColumn("total_debt", col("total_debt").cast("double")) \
        .withColumn("created_at", (col("created_at").cast("double") / 1000000.0).cast("timestamp"))
        
    write_to_clickhouse_and_delta(df_cleaned, "silver_postgres_customers")

def transform_postgres_cards(spark, execution_date=None):
    print("Transforming Postgres Cards...")
    path = get_input_path("postgres.public.cards", execution_date)
    try:
        df = spark.read.parquet(path)
    except Exception as e:
        if "PATH_NOT_FOUND" in str(e) or "Path does not exist" in str(e):
            print(f"No raw data found on S3 for postgres.public.cards yet. Skipping.")
            return
        raise e
    
    # De-duplicate
    window_spec = Window.partitionBy("id").orderBy(col("created_at").desc())
    df_dedup = df.withColumn("row_num", row_number().over(window_spec)) \
                 .filter(col("row_num") == 1) \
                 .drop("row_num")
                 
    df_cleaned = df_dedup \
        .withColumn("id", col("id").cast("integer")) \
        .withColumn("customer_id", col("customer_id").cast("integer")) \
        .withColumn("card_index", col("card_index").cast("integer")) \
        .withColumn("cards_issued", col("cards_issued").cast("integer")) \
        .withColumn("credit_limit", col("credit_limit").cast("double")) \
        .withColumn("year_pin_last_changed", col("year_pin_last_changed").cast("integer")) \
        .withColumn("created_at", (col("created_at").cast("double") / 1000000.0).cast("timestamp"))
        
    write_to_clickhouse_and_delta(df_cleaned, "silver_postgres_cards")

def transform_postgres_transactions(spark, execution_date=None):
    print("Transforming Postgres Transactions...")
    path = get_input_path("postgres.public.transactions", execution_date)
    try:
        df = spark.read.parquet(path)
    except Exception as e:
        if "PATH_NOT_FOUND" in str(e) or "Path does not exist" in str(e):
            print(f"No raw data found on S3 for postgres.public.transactions yet. Skipping.")
            return
        raise e
    
    # De-duplicate
    window_spec = Window.partitionBy("id").orderBy(col("transaction_date").desc())
    df_dedup = df.withColumn("row_num", row_number().over(window_spec)) \
                 .filter(col("row_num") == 1) \
                 .drop("row_num")
                 
    df_cleaned = df_dedup \
        .withColumn("id", col("id").cast("long")) \
        .withColumn("card_id", col("card_id").cast("integer")) \
        .withColumn("amount", col("amount").cast("double")) \
        .withColumn("mcc", col("mcc").cast("integer")) \
        .withColumn("year", col("year").cast("integer")) \
        .withColumn("month", col("month").cast("integer")) \
        .withColumn("day", col("day").cast("integer")) \
        .withColumn("transaction_date", (col("transaction_date").cast("double") / 1000000.0).cast("timestamp"))
        
    write_to_clickhouse_and_delta(df_cleaned, "silver_postgres_transactions")

MONGO_SCHEMAS = {
    "login_events": [
        ("_id", "string"),
        ("user_id", "long"),
        ("timestamp", "timestamp"),
        ("ip_address", "string"),
        ("device_type", "string"),
        ("status", "string"),
        ("event_source", "string"),
        ("location", "string")
    ],
    "device_events": [
        ("_id", "string"),
        ("user_id", "long"),
        ("device_id", "string"),
        ("os", "string"),
        ("app_version", "string"),
        ("event_type", "string"),
        ("event_source", "string"),
        ("location", "string"),
        ("timestamp", "timestamp")
    ],
    "fraud_events": [
        ("_id", "string"),
        ("transaction_id", "long"),
        ("customer_id", "long"),
        ("card_id", "long"),
        ("amount", "double"),
        ("merchant", "string"),
        ("transaction_date", "string"),
        ("risk_score", "integer"),
        ("fraud_reason", "string"),
        ("status", "string"),
        ("reported_at", "string")
    ],
    "notification_logs": [
        ("_id", "string"),
        ("customer_id", "long"),
        ("type", "string"),
        ("channel", "string"),
        ("message_body", "string"),
        ("timestamp", "string"),
        ("status", "string")
    ],
    "audit_logs": [
        ("_id", "string"),
        ("action", "string"),
        ("target_type", "string"),
        ("target_id", "long"),
        ("timestamp", "string"),
        ("details", "string")
    ]
}

def transform_mongo_events(spark, topic_name, table_name, execution_date=None):
    print(f"Transforming Mongo Events for {topic_name}...")
    path = get_input_path(f"mongo.banking_events.{topic_name}", execution_date)
    try:
        df = spark.read.parquet(path)
    except Exception as e:
        if "PATH_NOT_FOUND" in str(e) or "Path does not exist" in str(e):
            print(f"No raw data found on S3 for {topic_name} yet. Skipping.")
            return
        else:
            raise e
            
    from pyspark.sql.functions import lit
    
    # Đồng bộ hóa schema với ClickHouse
    schema_cols = MONGO_SCHEMAS[topic_name]
    for col_name, col_type in schema_cols:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None).cast(col_type))
        else:
            df = df.withColumn(col_name, col(col_name).cast(col_type))
            
    # Lựa chọn đúng thứ tự các cột như ClickHouse yêu cầu
    df_cleaned = df.select([col_name for col_name, _ in schema_cols])
    
    write_to_clickhouse_and_delta(df_cleaned, table_name)

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution-date", help="Execution date in YYYY-MM-DD format", default=None)
    args, unknown = parser.parse_known_args()
    
    execution_date = args.execution_date
    print(f"Running transformation with execution date: {execution_date}")

    spark = create_spark_session()
    try:
        transform_postgres_customers(spark, execution_date)
        transform_postgres_cards(spark, execution_date)
        transform_postgres_transactions(spark, execution_date)
        
        transform_mongo_events(spark, "login_events", "silver_mongo_login_events", execution_date)
        transform_mongo_events(spark, "device_events", "silver_mongo_device_events", execution_date)
        transform_mongo_events(spark, "fraud_events", "silver_mongo_fraud_events", execution_date)
        transform_mongo_events(spark, "notification_logs", "silver_mongo_notification_logs", execution_date)
        transform_mongo_events(spark, "audit_logs", "silver_mongo_audit_logs", execution_date)
        
        print("Silver Transformation successfully completed!")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
