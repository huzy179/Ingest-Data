from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max, row_number, when
from pyspark.sql.window import Window

def create_spark_session():
    spark = SparkSession.builder \
        .appName("Data-Lakehouse-Gold-Transformation") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio_admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio_password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark

def read_from_delta(spark, table_name):
    # Đọc dữ liệu từ Tầng Delta Lake Silver trên MinIO
    delta_path = f"s3a://banking-lakehouse/silver/delta/{table_name}"
    print(f"Reading from Delta Table: {delta_path}")
    return spark.read.format("delta").load(delta_path)

def write_to_clickhouse(df, table_name):
    # Điền chuỗi rỗng cho các cột string bị null để tránh lỗi nullability của ClickHouse
    df_filled = df.na.fill("")
    
    # Tự động chuyển đổi các cột struct hoặc array thành JSON string để ClickHouse JDBC nhận dạng được
    from pyspark.sql.types import StructType, ArrayType
    from pyspark.sql.functions import to_json
    for field in df_filled.schema.fields:
        if isinstance(field.dataType, (StructType, ArrayType)):
            df_filled = df_filled.withColumn(field.name, to_json(col(field.name)))
            
    jdbc_url = "jdbc:clickhouse://clickhouse:8123/analytics"
    
    # Truncate table first using JVM JDBC connection to preserve strict ClickHouse Nullable schema
    spark = df.sparkSession
    try:
        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, "default", "admin")
        stmt = conn.createStatement()
        stmt.execute(f"TRUNCATE TABLE {table_name}")
        stmt.close()
        conn.close()
        print(f"Successfully truncated ClickHouse table: analytics.{table_name}")
    except Exception as e:
        print(f"Warning: Failed to truncate ClickHouse table {table_name}: {e}")
        
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

def build_gold_fraud_analysis(spark):
    print("Building Gold Fraud Analysis Table...")
    
    # Đọc dữ liệu từ tầng Silver Delta Lake trên MinIO (Source of Truth)
    df_tx = read_from_delta(spark, "silver_postgres_transactions")
    df_cards = read_from_delta(spark, "silver_postgres_cards")
    df_cust = read_from_delta(spark, "silver_postgres_customers")
    df_fraud = read_from_delta(spark, "silver_mongo_fraud_events")
    df_notif = read_from_delta(spark, "silver_mongo_notification_logs")
    
    # 1. Join Transactions với Cards và Customers
    df_enriched = df_tx.join(df_cards, df_tx.card_id == df_cards.id, "inner") \
                       .join(df_cust, df_cards.customer_id == df_cust.id, "inner") \
                       .select(
                           df_tx.id.alias("transaction_id"),
                           df_cust.id.alias("customer_id"),
                           df_cust.name.alias("customer_name"),
                           df_cust.email.alias("customer_email"),
                           df_cust.phone.alias("customer_phone"),
                           df_cust.fico_score,
                           df_cards.card_brand,
                           df_cards.card_type,
                           df_cards.credit_limit,
                           df_tx.amount,
                           df_tx.use_chip,
                           df_tx.merchant_name,
                           df_tx.merchant_city,
                           df_tx.merchant_state,
                           df_tx.is_fraud,
                           df_tx.transaction_date
                       )
                       
    # 2. Join với Mongo Fraud Events (để lấy risk_score và fraud_reason)
    df_with_risk = df_enriched.join(df_fraud, df_enriched.transaction_id == df_fraud.transaction_id, "left") \
                              .select(
                                  df_enriched["*"],
                                  col("risk_score"),
                                  col("fraud_reason")
                              )
                              
    # 3. Join với Notification Logs (lấy trạng thái thông báo sms/email nếu có)
    window_spec = Window.partitionBy("customer_id").orderBy(col("timestamp").desc())
    df_latest_notif = df_notif.withColumn("rn", row_number().over(window_spec)) \
                              .filter(col("rn") == 1) \
                              .select("customer_id", col("type").alias("alert_type"), col("channel").alias("alert_channel"), col("status").alias("alert_status"))
                              
    df_final = df_with_risk.join(df_latest_notif, "customer_id", "left")
    
    # Điền giá trị mặc định để xử lý triệt để null do left join
    df_final = df_final.na.fill({
        "credit_limit": 0.0,
        "card_brand": "Unknown",
        "card_type": "Unknown",
        "risk_score": -1,
        "fraud_reason": "Not Flagged",
        "alert_type": "None",
        "alert_channel": "None",
        "alert_status": "None",
        "fico_score": -1
    })
    
    write_to_clickhouse(df_final, "gold_fraud_analysis")

def build_gold_user_behavior_summary(spark):
    print("Building Gold User Behavior Summary...")
    
    # Đọc dữ liệu từ tầng Silver Delta Lake trên MinIO
    df_tx = read_from_delta(spark, "silver_postgres_transactions")
    df_cards = read_from_delta(spark, "silver_postgres_cards")
    df_cust = read_from_delta(spark, "silver_postgres_customers")
    df_logins = read_from_delta(spark, "silver_mongo_login_events")
    df_devices = read_from_delta(spark, "silver_mongo_device_events")
    
    # 1. Tính toán Transaction metrics
    df_tx_stats = df_tx.join(df_cards, df_tx.card_id == df_cards.id, "inner") \
                       .groupBy("customer_id") \
                       .agg(
                           count(df_tx.id).alias("total_transactions"),
                           sum("amount").alias("total_amount_spent"),
                           avg("amount").alias("average_transaction_amount"),
                           sum(when(col("is_fraud") == "Yes", 1).otherwise(0)).alias("total_fraud_transactions")
                       )
                       
    # 2. Tính failed logins
    df_login_stats = df_logins.groupBy("user_id") \
                              .agg(
                                  sum(when(col("status") == "Failed", 1).otherwise(0)).alias("total_failed_logins"),
                                  count("status").alias("total_logins")
                              )
                              
    # 3. Xác định hệ điều hành (OS) chính của thiết bị
    window_spec = Window.partitionBy("user_id").orderBy(col("os_cnt").desc())
    df_os_counts = df_devices.groupBy("user_id", "os").agg(count("os").alias("os_cnt"))
    df_primary_os = df_os_counts.withColumn("rn", row_number().over(window_spec)) \
                               .filter(col("rn") == 1) \
                               .select("user_id", col("os").alias("primary_device_os"))
                               
    # 4. Join tất cả với Customers
    df_gold = df_cust.join(df_tx_stats, df_cust.id == df_tx_stats.customer_id, "left") \
                      .join(df_login_stats, df_cust.id == df_login_stats.user_id, "left") \
                      .join(df_primary_os, df_cust.id == df_primary_os.user_id, "left") \
                      .select(
                          df_cust.id.alias("customer_id"),
                          df_cust.name.alias("customer_name"),
                          df_cust.state,
                          df_cust.yearly_income,
                          df_cust.total_debt,
                          col("total_transactions").cast("long"),
                          col("total_amount_spent"),
                          col("average_transaction_amount"),
                          col("total_fraud_transactions").cast("long"),
                          col("total_failed_logins").cast("long"),
                          col("primary_device_os")
                      ).na.fill({
                          "total_transactions": 0,
                          "total_amount_spent": 0.0,
                          "average_transaction_amount": 0.0,
                          "total_fraud_transactions": 0,
                          "total_failed_logins": 0,
                          "primary_device_os": "Unknown"
                      })
                      
    write_to_clickhouse(df_gold, "gold_user_behavior_summary")

def main():
    spark = create_spark_session()
    try:
        build_gold_fraud_analysis(spark)
        build_gold_user_behavior_summary(spark)
        print("Gold Transformation successfully completed!")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
