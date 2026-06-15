import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, to_timestamp, when, lit, to_json
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType, TimestampType, ArrayType

def create_spark_session():
    # Khởi tạo Spark Session có tích hợp Delta Lake Extension và Catalog
    spark = SparkSession.builder \
        .appName("Data-Lakehouse-Silver-Streaming") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio_admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio_password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark

def write_to_clickhouse_and_delta(df, batch_id, table_name):
    # Đếm số bản ghi trong batch
    cnt = df.count()
    if cnt == 0:
        return
        
    print(f"Streaming batch {batch_id}: processing {cnt} rows for {table_name}")
    
    # 1. Ghi dữ liệu vào Delta Lake
    delta_path = f"s3a://banking-lakehouse/silver/delta/{table_name}"
    try:
        df.write \
            .format("delta") \
            .mode("append") \
            .save(delta_path)
        print(f"Successfully wrote batch to Delta Lake: {delta_path}")
    except Exception as e:
        print(f"Error writing batch to Delta Lake for {table_name}: {e}")

    # 2. Ghi dữ liệu vào ClickHouse
    df_filled = df.na.fill("")
    for field in df_filled.schema.fields:
        if isinstance(field.dataType, (StructType, ArrayType)):
            df_filled = df_filled.withColumn(field.name, to_json(col(field.name)))
            
    jdbc_url = "jdbc:clickhouse://clickhouse:8123/analytics"
    try:
        df_filled.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", "default") \
            .option("password", "admin") \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .mode("append") \
            .save()
        print(f"Successfully wrote batch to ClickHouse: analytics.{table_name}")
    except Exception as e:
        print(f"Error writing batch to ClickHouse for {table_name}: {e}")

# --- Định nghĩa Schema tĩnh cho việc đọc Stream Parquet ---
SCHEMAS = {
    "customers": StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("current_age", IntegerType(), True),
        StructField("retirement_age", IntegerType(), True),
        StructField("birth_year", IntegerType(), True),
        StructField("birth_month", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("address", StringType(), True),
        StructField("apartment", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zipcode", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("per_capita_income_zipcode", DoubleType(), True),
        StructField("yearly_income", DoubleType(), True),
        StructField("total_debt", DoubleType(), True),
        StructField("fico_score", IntegerType(), True),
        StructField("num_credit_cards", IntegerType(), True),
        StructField("created_at", DoubleType(), True)
    ]),
    "cards": StructType([
        StructField("id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("card_index", IntegerType(), True),
        StructField("card_brand", StringType(), True),
        StructField("card_type", StringType(), True),
        StructField("card_number", StringType(), True),
        StructField("expires", StringType(), True),
        StructField("cvv", StringType(), True),
        StructField("has_chip", StringType(), True),
        StructField("cards_issued", IntegerType(), True),
        StructField("credit_limit", DoubleType(), True),
        StructField("acct_open_date", StringType(), True),
        StructField("year_pin_last_changed", IntegerType(), True),
        StructField("card_on_dark_web", StringType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", DoubleType(), True)
    ]),
    "transactions": StructType([
        StructField("id", LongType(), True),
        StructField("card_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("use_chip", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_city", StringType(), True),
        StructField("merchant_state", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("mcc", IntegerType(), True),
        StructField("errors", StringType(), True),
        StructField("is_fraud", StringType(), True),
        StructField("transaction_date", DoubleType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("description", StringType(), True)
    ]),
    "login_events": StructType([
        StructField("_id", StringType(), True),
        StructField("user_id", LongType(), True),
        StructField("timestamp", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("event_source", StringType(), True),
        StructField("location", StringType(), True)
    ]),
    "device_events": StructType([
        StructField("_id", StringType(), True),
        StructField("user_id", LongType(), True),
        StructField("device_id", StringType(), True),
        StructField("os", StringType(), True),
        StructField("app_version", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_source", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", StringType(), True)
    ]),
    "fraud_events": StructType([
        StructField("_id", StringType(), True),
        StructField("transaction_id", LongType(), True),
        StructField("customer_id", LongType(), True),
        StructField("card_id", LongType(), True),
        StructField("amount", DoubleType(), True),
        StructField("merchant", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("risk_score", IntegerType(), True),
        StructField("fraud_reason", StringType(), True),
        StructField("status", StringType(), True),
        StructField("reported_at", StringType(), True)
    ]),
    "notification_logs": StructType([
        StructField("_id", StringType(), True),
        StructField("customer_id", LongType(), True),
        StructField("type", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("message_body", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("status", StringType(), True)
    ]),
    "audit_logs": StructType([
        StructField("_id", StringType(), True),
        StructField("action", StringType(), True),
        StructField("target_type", StringType(), True),
        StructField("target_id", LongType(), True),
        StructField("timestamp", StringType(), True),
        StructField("details", StringType(), True)
    ])
}

def start_postgres_stream(spark, topic_name, table_name, schema):
    input_path = f"s3a://banking-lakehouse/topics/{topic_name}/year=*/month=*/day=*/*.parquet"
    checkpoint_path = f"s3a://banking-lakehouse/checkpoints/{table_name}"
    
    print(f"Starting Parquet stream for {topic_name} -> {table_name}")
    
    df_stream = spark.readStream \
        .schema(schema) \
        .parquet(input_path)
        
    if "created_at" in df_stream.columns:
        df_stream = df_stream.withColumn("created_at", (col("created_at") / 1000000.0).cast(TimestampType()))
    if "transaction_date" in df_stream.columns:
        df_stream = df_stream.withColumn("transaction_date", (col("transaction_date") / 1000000.0).cast(TimestampType()))
        
    query = df_stream.writeStream \
        .foreachBatch(lambda batch_df, batch_id: write_to_clickhouse_and_delta(batch_df, batch_id, table_name)) \
        .option("checkpointLocation", checkpoint_path) \
        .start()
        
    return query

def start_mongo_stream(spark, topic_name, table_name, schema):
    input_path = f"s3a://banking-lakehouse/topics/mongo.banking_events.{topic_name}/year=*/month=*/day=*/*.parquet"
    checkpoint_path = f"s3a://banking-lakehouse/checkpoints/{table_name}"
    
    print(f"Starting Parquet stream for MongoDB: {topic_name} -> {table_name}")
    
    df_stream = spark.readStream \
        .schema(schema) \
        .parquet(input_path)
        
    if "timestamp" in df_stream.columns:
        df_stream = df_stream.withColumn("timestamp", col("timestamp").cast(TimestampType()))
        
    query = df_stream.writeStream \
        .foreachBatch(lambda batch_df, batch_id: write_to_clickhouse_and_delta(batch_df, batch_id, table_name)) \
        .option("checkpointLocation", checkpoint_path) \
        .start()
        
    return query

def main():
    spark = create_spark_session()
    queries = []
    
    # 1. Các stream từ PostgreSQL
    queries.append(start_postgres_stream(spark, "postgres.public.customers", "silver_postgres_customers", SCHEMAS["customers"]))
    queries.append(start_postgres_stream(spark, "postgres.public.cards", "silver_postgres_cards", SCHEMAS["cards"]))
    queries.append(start_postgres_stream(spark, "postgres.public.transactions", "silver_postgres_transactions", SCHEMAS["transactions"]))
    
    # 2. Các stream từ MongoDB
    queries.append(start_mongo_stream(spark, "login_events", "silver_mongo_login_events", SCHEMAS["login_events"]))
    queries.append(start_mongo_stream(spark, "device_events", "silver_mongo_device_events", SCHEMAS["device_events"]))
    queries.append(start_mongo_stream(spark, "fraud_events", "silver_mongo_fraud_events", SCHEMAS["fraud_events"]))
    queries.append(start_mongo_stream(spark, "notification_logs", "silver_mongo_notification_logs", SCHEMAS["notification_logs"]))
    queries.append(start_mongo_stream(spark, "audit_logs", "silver_mongo_audit_logs", SCHEMAS["audit_logs"]))
    
    print("All Structured Streaming queries have started. Awaiting termination...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
