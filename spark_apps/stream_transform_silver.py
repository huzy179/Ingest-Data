import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, to_timestamp, when, lit, to_json, expr
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType, TimestampType, ArrayType, BooleanType

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
        .config("spark.databricks.delta.autoCompact.enabled", "true") \
        .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
        .getOrCreate()
    return spark

def write_to_delta(df, batch_id, table_name):
    # Đếm số bản ghi trong batch
    cnt = df.count()
    if cnt == 0:
        return
        
    print(f"Streaming batch {batch_id}: processing {cnt} rows for {table_name}")
    
    delta_path = f"s3a://banking-lakehouse/silver/delta/{table_name}"
    spark = df.sparkSession
    
    from delta.tables import DeltaTable
    
    # Xác định khóa chính (PostgreSQL dùng id, MongoDB dùng _id)
    primary_key = "_id" if "_id" in df.columns else "id"
    
    try:
        # Nếu bảng đã tồn tại, thực hiện MERGE INTO để tránh trùng lặp dữ liệu
        if DeltaTable.isDeltaTable(spark, delta_path):
            delta_table = DeltaTable.forPath(spark, delta_path)
            
            # Upsert (Merge) dữ liệu mới dựa trên Khóa chính
            delta_table.alias("target").merge(
                source = df.alias("source"),
                condition = f"target.{primary_key} = source.{primary_key}"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
            print(f"Successfully merged batch to Delta Lake table: {delta_path}")
        else:
            # Ghi đè khởi tạo bảng mới
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(delta_path)
            print(f"Successfully initialized Delta Lake table at: {delta_path}")
    except Exception as e:
        print(f"Error merging batch to Delta Lake for {table_name}: {e}")

# --- Hàm helper chuyển đổi Spark StructType thành Avro schema JSON string ---
def type_to_avro_type(dataType):
    if isinstance(dataType, StringType):
        return ["null", "string"]
    elif isinstance(dataType, LongType):
        return ["null", "long"]
    elif isinstance(dataType, IntegerType):
        return ["null", "int"]
    elif isinstance(dataType, DoubleType):
        return ["null", "double"]
    elif isinstance(dataType, TimestampType):
        return ["null", {"type": "long", "logicalType": "timestamp-micros"}]
    elif isinstance(dataType, BooleanType):
        return ["null", "boolean"]
    else:
        return ["null", "string"]

def struct_to_avro_schema(struct, name):
    fields = []
    for field in struct.fields:
        fields.append({
            "name": field.name,
            "type": type_to_avro_type(field.dataType),
            "default": None
        })
    schema = {
        "type": "record",
        "name": name,
        "namespace": "avro",
        "fields": fields
    }
    return json.dumps(schema)

# --- Định nghĩa Schema tĩnh cho việc đọc Stream ---
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

def fetch_schema_from_registry(subject_name):
    import urllib.request
    import json
    url = f"http://schema-registry:8081/subjects/{subject_name}/versions/latest"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=5) as response:
            data = json.loads(response.read().decode('utf-8'))
            return data["schema"]
    except Exception as e:
        print(f"Error fetching schema for {subject_name} from registry: {e}")
        return None

def start_stream(spark, topic_name, table_name):
    print(f"Starting Streaming from Kafka: {topic_name} -> {table_name}")
    
    # 1. Đọc stream từ Kafka Broker trực tiếp
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .load()
        
    # Lọc bỏ các tin nhắn rỗng (tombstones / null values) để tránh lỗi parse
    df_non_null = df_raw.filter(col("value").isNotNull())
        
    # 2. Lấy schema tự động từ Schema Registry
    subject_name = f"{topic_name}-value"
    avro_schema = fetch_schema_from_registry(subject_name)
    if not avro_schema:
        raise ValueError(f"Could not retrieve Avro schema for subject {subject_name}")
        
    # 3. Giải mã định dạng Avro từ Confluent (loại bỏ 5 bytes magic header)
    df_avro = df_non_null.withColumn("avro_payload", expr("substring(value, 6)"))
    from pyspark.sql.avro.functions import from_avro
    
    # Sử dụng mode PERMISSIVE để bỏ qua các dòng lỗi thay vì crash stream
    df_decoded = df_avro.withColumn("data", from_avro(col("avro_payload"), avro_schema, {"mode": "PERMISSIVE"})) \
                        .select("data.*")
        
    # 4. Ép kiểu và chuẩn hóa
    if "created_at" in df_decoded.columns:
        df_decoded = df_decoded.withColumn("created_at", (col("created_at") / 1000000.0).cast(TimestampType()))
    if "transaction_date" in df_decoded.columns:
        df_decoded = df_decoded.withColumn("transaction_date", (col("transaction_date") / 1000000.0).cast(TimestampType()))
    if "timestamp" in df_decoded.columns:
        df_decoded = df_decoded.withColumn("timestamp", col("timestamp").cast(TimestampType()))
        
    checkpoint_path = f"s3a://banking-lakehouse/checkpoints/{table_name}"
    
    query = df_decoded.writeStream \
        .foreachBatch(lambda batch_df, batch_id: write_to_delta(batch_df, batch_id, table_name)) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime='5 minutes') \
        .start()
        
    return query

def main():
    spark = create_spark_session()
    queries = []
    
    # 1. Khởi chạy các stream từ PostgreSQL
    queries.append(start_stream(spark, "postgres.public.customers", "silver_postgres_customers"))
    queries.append(start_stream(spark, "postgres.public.cards", "silver_postgres_cards"))
    queries.append(start_stream(spark, "postgres.public.transactions", "silver_postgres_transactions"))
    
    # 2. Khởi chạy các stream từ MongoDB
    queries.append(start_stream(spark, "mongo.banking_events.login_events", "silver_mongo_login_events"))
    queries.append(start_stream(spark, "mongo.banking_events.device_events", "silver_mongo_device_events"))
    queries.append(start_stream(spark, "mongo.banking_events.fraud_events", "silver_mongo_fraud_events"))
    queries.append(start_stream(spark, "mongo.banking_events.notification_logs", "silver_mongo_notification_logs"))
    queries.append(start_stream(spark, "mongo.banking_events.audit_logs", "silver_mongo_audit_logs"))
    
    print("All Kafka Structured Streaming queries have started. Awaiting termination...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
