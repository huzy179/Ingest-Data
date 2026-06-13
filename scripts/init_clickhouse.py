import urllib.request
import urllib.error
import time
import sys

CLICKHOUSE_URL = "http://localhost:8123/"

DDL_QUERIES = [
    # Create databases
    "CREATE DATABASE IF NOT EXISTS raw",
    "CREATE DATABASE IF NOT EXISTS analytics",
    
    # 1. raw.postgres_customers
    """
    CREATE TABLE IF NOT EXISTS raw.postgres_customers (
        id Nullable(Int64),
        name Nullable(String),
        email Nullable(String),
        phone Nullable(String),
        current_age Nullable(Int32),
        retirement_age Nullable(Int32),
        birth_year Nullable(Int32),
        birth_month Nullable(Int32),
        gender Nullable(String),
        address Nullable(String),
        apartment Nullable(String),
        city Nullable(String),
        state Nullable(String),
        zipcode Nullable(String),
        latitude Nullable(Float64),
        longitude Nullable(Float64),
        per_capita_income_zipcode Nullable(Float64),
        yearly_income Nullable(Float64),
        total_debt Nullable(Float64),
        fico_score Nullable(Int32),
        num_credit_cards Nullable(Int32),
        created_at Nullable(String)
    ) ENGINE = S3('http://minio:9000/banking-lakehouse/topics/postgres.public.customers/partition=*/*.json', 'minio_admin', 'minio_password', 'JSONEachRow');
    """,
    
    # 2. raw.postgres_cards
    """
    CREATE TABLE IF NOT EXISTS raw.postgres_cards (
        id Nullable(Int32),
        customer_id Nullable(Int32),
        card_index Nullable(Int32),
        card_brand Nullable(String),
        card_type Nullable(String),
        card_number Nullable(String),
        expires Nullable(String),
        cvv Nullable(String),
        has_chip Nullable(String),
        cards_issued Nullable(Int32),
        credit_limit Nullable(Float64),
        acct_open_date Nullable(String),
        year_pin_last_changed Nullable(Int32),
        card_on_dark_web Nullable(String),
        status Nullable(String),
        created_at Nullable(String)
    ) ENGINE = S3('http://minio:9000/banking-lakehouse/topics/postgres.public.cards/partition=*/*.json', 'minio_admin', 'minio_password', 'JSONEachRow');
    """,
    
    # 3. raw.postgres_transactions
    """
    CREATE TABLE IF NOT EXISTS raw.postgres_transactions (
        id Nullable(Int64),
        card_id Nullable(Int32),
        year Nullable(Int32),
        month Nullable(Int32),
        day Nullable(Int32),
        time Nullable(String),
        amount Nullable(Float64),
        use_chip Nullable(String),
        merchant_name Nullable(String),
        merchant_city Nullable(String),
        merchant_state Nullable(String),
        zip Nullable(String),
        mcc Nullable(Int32),
        errors Nullable(String),
        is_fraud Nullable(String),
        transaction_date Nullable(String),
        description Nullable(String)
    ) ENGINE = S3('http://minio:9000/banking-lakehouse/topics/postgres.public.transactions/partition=*/*.json', 'minio_admin', 'minio_password', 'JSONEachRow');
    """,
    
    # 4. raw.mongo_customers
    """
    CREATE TABLE IF NOT EXISTS raw.mongo_customers (
        _id Nullable(Int64),
        recent_transactions Nullable(String)
    ) ENGINE = S3('http://minio:9000/banking-lakehouse/topics/mongo.banking_events.customers/partition=*/*.json', 'minio_admin', 'minio_password', 'JSONEachRow');
    """,
    
    # 5. raw.mongo_login_events
    """
    CREATE TABLE IF NOT EXISTS raw.mongo_login_events (
        _id Nullable(String),
        user_id Nullable(Int64),
        timestamp Nullable(String),
        ip_address Nullable(String),
        device_type Nullable(String),
        status Nullable(String),
        event_source Nullable(String),
        location Nullable(String)
    ) ENGINE = S3('http://minio:9000/banking-lakehouse/topics/mongo.banking_events.login_events/partition=*/*.json', 'minio_admin', 'minio_password', 'JSONEachRow');
    """,
    
    # 6. raw.mongo_device_events
    """
    CREATE TABLE IF NOT EXISTS raw.mongo_device_events (
        _id Nullable(String),
        user_id Nullable(Int64),
        device_id Nullable(String),
        os Nullable(String),
        app_version Nullable(String),
        event_type Nullable(String),
        event_source Nullable(String),
        location Nullable(String),
        timestamp Nullable(String)
    ) ENGINE = S3('http://minio:9000/banking-lakehouse/topics/mongo.banking_events.device_events/partition=*/*.json', 'minio_admin', 'minio_password', 'JSONEachRow');
    """,
    
    # 7. raw.mongo_fraud_events
    """
    CREATE TABLE IF NOT EXISTS raw.mongo_fraud_events (
        _id Nullable(String),
        transaction_id Nullable(Int64),
        customer_id Nullable(Int64),
        card_id Nullable(Int64),
        amount Nullable(Float64),
        merchant Nullable(String),
        transaction_date Nullable(String),
        risk_score Nullable(Int32),
        fraud_reason Nullable(String),
        status Nullable(String),
        reported_at Nullable(String)
    ) ENGINE = S3('http://minio:9000/banking-lakehouse/topics/mongo.banking_events.fraud_events/partition=*/*.json', 'minio_admin', 'minio_password', 'JSONEachRow');
    """,
    
    # 8. raw.mongo_notification_logs
    """
    CREATE TABLE IF NOT EXISTS raw.mongo_notification_logs (
        _id Nullable(String),
        customer_id Nullable(Int64),
        type Nullable(String),
        channel Nullable(String),
        message_body Nullable(String),
        timestamp Nullable(String),
        status Nullable(String)
    ) ENGINE = S3('http://minio:9000/banking-lakehouse/topics/mongo.banking_events.notification_logs/partition=*/*.json', 'minio_admin', 'minio_password', 'JSONEachRow');
    """,
    
    # 9. raw.mongo_audit_logs
    """
    CREATE TABLE IF NOT EXISTS raw.mongo_audit_logs (
        _id Nullable(String),
        action Nullable(String),
        target_type Nullable(String),
        target_id Nullable(Int64),
        timestamp Nullable(String),
        details Nullable(String)
    ) ENGINE = S3('http://minio:9000/banking-lakehouse/topics/mongo.banking_events.audit_logs/partition=*/*.json', 'minio_admin', 'minio_password', 'JSONEachRow');
    """,
    
    # 10. analytics.silver_postgres_customers
    """
    CREATE TABLE IF NOT EXISTS analytics.silver_postgres_customers (
        id Nullable(Int64),
        name Nullable(String),
        email Nullable(String),
        phone Nullable(String),
        current_age Nullable(Int32),
        retirement_age Nullable(Int32),
        birth_year Nullable(Int32),
        birth_month Nullable(Int32),
        gender Nullable(String),
        address Nullable(String),
        apartment Nullable(String),
        city Nullable(String),
        state Nullable(String),
        zipcode Nullable(String),
        latitude Nullable(Float64),
        longitude Nullable(Float64),
        per_capita_income_zipcode Nullable(Float64),
        yearly_income Nullable(Float64),
        total_debt Nullable(Float64),
        fico_score Nullable(Int32),
        num_credit_cards Nullable(Int32),
        created_at Nullable(DateTime)
    ) ENGINE = MergeTree() ORDER BY id SETTINGS allow_nullable_key = 1;
    """,
    
    # 11. analytics.silver_postgres_cards
    """
    CREATE TABLE IF NOT EXISTS analytics.silver_postgres_cards (
        id Nullable(Int32),
        customer_id Nullable(Int32),
        card_index Nullable(Int32),
        card_brand Nullable(String),
        card_type Nullable(String),
        card_number Nullable(String),
        expires Nullable(String),
        cvv Nullable(String),
        has_chip Nullable(String),
        cards_issued Nullable(Int32),
        credit_limit Nullable(Float64),
        acct_open_date Nullable(String),
        year_pin_last_changed Nullable(Int32),
        card_on_dark_web Nullable(String),
        status Nullable(String),
        created_at Nullable(DateTime)
    ) ENGINE = MergeTree() ORDER BY (customer_id, card_index) SETTINGS allow_nullable_key = 1;
    """,
    
    # 12. analytics.silver_postgres_transactions
    """
    CREATE TABLE IF NOT EXISTS analytics.silver_postgres_transactions (
        id Nullable(Int64),
        card_id Nullable(Int32),
        year Nullable(Int32),
        month Nullable(Int32),
        day Nullable(Int32),
        time Nullable(String),
        amount Nullable(Float64),
        use_chip Nullable(String),
        merchant_name Nullable(String),
        merchant_city Nullable(String),
        merchant_state Nullable(String),
        zip Nullable(String),
        mcc Nullable(Int32),
        errors Nullable(String),
        is_fraud Nullable(String),
        transaction_date Nullable(DateTime),
        description Nullable(String)
    ) ENGINE = MergeTree() ORDER BY (card_id, transaction_date) SETTINGS allow_nullable_key = 1;
    """,
    
    # 13. analytics.silver_mongo_login_events
    """
    CREATE TABLE IF NOT EXISTS analytics.silver_mongo_login_events (
        _id Nullable(String),
        user_id Nullable(Int64),
        timestamp Nullable(DateTime),
        ip_address Nullable(String),
        device_type Nullable(String),
        status Nullable(String),
        event_source Nullable(String),
        location Nullable(String)
    ) ENGINE = MergeTree() ORDER BY (user_id, timestamp) SETTINGS allow_nullable_key = 1;
    """,
    
    # 14. analytics.silver_mongo_device_events
    """
    CREATE TABLE IF NOT EXISTS analytics.silver_mongo_device_events (
        _id Nullable(String),
        user_id Nullable(Int64),
        device_id Nullable(String),
        os Nullable(String),
        app_version Nullable(String),
        event_type Nullable(String),
        event_source Nullable(String),
        location Nullable(String),
        timestamp Nullable(DateTime)
    ) ENGINE = MergeTree() ORDER BY (user_id, timestamp) SETTINGS allow_nullable_key = 1;
    """,
    
    # 15. analytics.silver_mongo_fraud_events
    """
    CREATE TABLE IF NOT EXISTS analytics.silver_mongo_fraud_events (
        _id Nullable(String),
        transaction_id Nullable(Int64),
        customer_id Nullable(Int64),
        card_id Nullable(Int64),
        amount Nullable(Float64),
        merchant Nullable(String),
        transaction_date Nullable(String),
        risk_score Nullable(Int32),
        fraud_reason Nullable(String),
        status Nullable(String),
        reported_at Nullable(String)
    ) ENGINE = MergeTree() ORDER BY (customer_id, transaction_date) SETTINGS allow_nullable_key = 1;
    """,
    
    # 16. analytics.silver_mongo_notification_logs
    """
    CREATE TABLE IF NOT EXISTS analytics.silver_mongo_notification_logs (
        _id Nullable(String),
        customer_id Nullable(Int64),
        type Nullable(String),
        channel Nullable(String),
        message_body Nullable(String),
        timestamp Nullable(String),
        status Nullable(String)
    ) ENGINE = MergeTree() ORDER BY (customer_id, timestamp) SETTINGS allow_nullable_key = 1;
    """,
    
    # 17. analytics.silver_mongo_audit_logs
    """
    CREATE TABLE IF NOT EXISTS analytics.silver_mongo_audit_logs (
        _id Nullable(String),
        action Nullable(String),
        target_type Nullable(String),
        target_id Nullable(Int64),
        timestamp Nullable(String),
        details Nullable(String)
    ) ENGINE = MergeTree() ORDER BY (target_id, timestamp) SETTINGS allow_nullable_key = 1;
    """,
    
    # 18. analytics.gold_fraud_analysis
    """
    CREATE TABLE IF NOT EXISTS analytics.gold_fraud_analysis (
        customer_id Nullable(Int64),
        transaction_id Nullable(Int64),
        customer_name Nullable(String),
        customer_email Nullable(String),
        customer_phone Nullable(String),
        fico_score Nullable(Int32),
        card_brand Nullable(String),
        card_type Nullable(String),
        credit_limit Nullable(Float64),
        amount Nullable(Float64),
        use_chip Nullable(String),
        merchant_name Nullable(String),
        merchant_city Nullable(String),
        merchant_state Nullable(String),
        is_fraud Nullable(String),
        transaction_date Nullable(DateTime),
        risk_score Nullable(Int32),
        fraud_reason Nullable(String),
        alert_type Nullable(String),
        alert_channel Nullable(String),
        alert_status Nullable(String)
    ) ENGINE = MergeTree() ORDER BY (customer_id, transaction_date) SETTINGS allow_nullable_key = 1;
    """,
    
    # 19. analytics.gold_user_behavior_summary
    """
    CREATE TABLE IF NOT EXISTS analytics.gold_user_behavior_summary (
        customer_id Nullable(Int64),
        customer_name Nullable(String),
        state Nullable(String),
        yearly_income Nullable(Float64),
        total_debt Nullable(Float64),
        total_transactions Nullable(Int64),
        total_amount_spent Nullable(Float64),
        average_transaction_amount Nullable(Float64),
        total_fraud_transactions Nullable(Int64),
        total_failed_logins Nullable(Int64),
        primary_device_os Nullable(String)
    ) ENGINE = MergeTree() ORDER BY customer_id SETTINGS allow_nullable_key = 1;
    """
]

def wait_for_clickhouse():
    print("Waiting for ClickHouse to be ready at http://localhost:8123...")
    retries = 30
    while retries > 0:
        try:
            req = urllib.request.Request(CLICKHOUSE_URL, data=b"SELECT 1")
            req.add_header("X-ClickHouse-User", "default")
            req.add_header("X-ClickHouse-Key", "admin")
            with urllib.request.urlopen(req, timeout=3) as response:
                if response.status == 200:
                    val = response.read().decode("utf-8").strip()
                    if val == "1":
                        print("ClickHouse is ready!")
                        return True
        except Exception:
            pass
        retries -= 1
        time.sleep(2)
    print("Error: ClickHouse did not start in time.")
    return False

def execute_query(query):
    try:
        req = urllib.request.Request(CLICKHOUSE_URL, data=query.encode("utf-8"))
        req.add_header("X-ClickHouse-User", "default")
        req.add_header("X-ClickHouse-Key", "admin")
        with urllib.request.urlopen(req) as response:
            if response.status == 200:
                return True
    except urllib.error.HTTPError as e:
        print(f"HTTP Error executing query: {e.code} - {e.read().decode('utf-8')}")
    except Exception as e:
        print(f"Error executing query: {e}")
    return False

def main():
    if not wait_for_clickhouse():
        sys.exit(1)
        
    print("\nInitializing ClickHouse databases and tables...")
    for idx, query in enumerate(DDL_QUERIES):
        query_clean = " ".join(query.strip().split())
        # Print a short preview of the query
        preview = query_clean[:60] + "..." if len(query_clean) > 60 else query_clean
        print(f"Running query {idx+1}/{len(DDL_QUERIES)}: {preview}")
        if not execute_query(query):
            print(f"Error executing query: {query}")
            sys.exit(1)
            
    print("\nClickHouse initialization completed successfully!")

if __name__ == "__main__":
    main()
