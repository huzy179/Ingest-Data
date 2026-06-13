import os
import pandas as pd
from sqlalchemy import create_engine, text
from pymongo import MongoClient, UpdateOne
import datetime
import sys
import time
import random

# Database connections
PG_URL = "postgresql+psycopg://admin:admin@localhost:5434/banking_core"
MONGO_URL = "mongodb://admin:admin@localhost:27017/?directConnection=true"

pg_engine = create_engine(PG_URL)
mongo_client = MongoClient(MONGO_URL)
mongo_db = mongo_client["banking_events"]

mongo_customers_col = mongo_db["customers"]
mongo_fraud_col = mongo_db["fraud_events"]
mongo_notification_col = mongo_db["notification_logs"]
mongo_login_col = mongo_db["login_events"]
mongo_device_col = mongo_db["device_events"]

csv_path = "data/transactions.csv"

# 1. Check if CSV exists
if not os.path.exists(csv_path):
    raise FileNotFoundError(f"File '{csv_path}' không tồn tại. Vui lòng đặt file CSV của bạn tại đây để import.")

# 2. Cache cards to avoid querying database for every row
print("Caching cards from PostgreSQL...")
cards_cache = {}
with pg_engine.connect() as conn:
    res = conn.execute(text("SELECT id, customer_id, card_index FROM cards"))
    for row in res:
        # key: (customer_id, card_index) -> value: card_id
        cards_cache[(row[1], row[2])] = row[0]
print(f"Cached {len(cards_cache)} cards.")

if len(cards_cache) == 0:
    print("Warning: No cards found in PostgreSQL database. Please run import_cards.py first!")
    sys.exit(1)

# Clean up MongoDB collections before starting
print("Cleaning old events and customer profiles in MongoDB...")
mongo_customers_col.update_many({}, {"$set": {"recent_transactions": []}})
mongo_fraud_col.delete_many({})
mongo_notification_col.delete_many({"channel": "sms"})
mongo_login_col.delete_many({"event_source": "transaction_flow"})
mongo_device_col.delete_many({"event_source": "transaction_flow"})

# SQL statement for batch insertion
insert_sql = text("""
    INSERT INTO transactions (
        id, card_id, year, month, day, time, amount, use_chip, 
        merchant_name, merchant_city, merchant_state, zip, mcc, 
        errors, is_fraud, transaction_date, description
    )
    VALUES (
        :id, :card_id, :year, :month, :day, :time, :amount, :use_chip, 
        :merchant_name, :merchant_city, :merchant_state, :zip, :mcc, 
        :errors, :is_fraud, :transaction_date, :description
    )
    ON CONFLICT (id) DO NOTHING;
""")

# Global variables for processing
global_tx_id_counter = 0
total_users_processed = 0

pg_insert_queue = []
mongo_customer_updates = []
mongo_fraud_queue = []
mongo_notification_queue = []
mongo_login_queue = []
mongo_device_queue = []

def flush_queues(force=False):
    global pg_insert_queue, mongo_customer_updates, mongo_fraud_queue, mongo_notification_queue, mongo_login_queue, mongo_device_queue
    
    # 1. Bulk insert to PostgreSQL
    if len(pg_insert_queue) >= 50000 or (force and pg_insert_queue):
        print(f"Inserting {len(pg_insert_queue)} transactions into PostgreSQL...")
        with pg_engine.begin() as conn:
            conn.execute(insert_sql, pg_insert_queue)
        pg_insert_queue = []
        
    # 2. Bulk write to MongoDB Customer Profiles
    if len(mongo_customer_updates) >= 500 or (force and mongo_customer_updates):
        print(f"Updating {len(mongo_customer_updates)} customer profiles in MongoDB...")
        mongo_customers_col.bulk_write(mongo_customer_updates)
        mongo_customer_updates = []
        
    # 3. Bulk insert to MongoDB fraud_events
    if len(mongo_fraud_queue) >= 500 or (force and mongo_fraud_queue):
        print(f"Inserting {len(mongo_fraud_queue)} fraud events into MongoDB...")
        mongo_fraud_col.insert_many(mongo_fraud_queue)
        mongo_fraud_queue = []
        
    # 4. Bulk insert to MongoDB notification_logs
    if len(mongo_notification_queue) >= 1000 or (force and mongo_notification_queue):
        print(f"Inserting {len(mongo_notification_queue)} notification logs into MongoDB...")
        mongo_notification_col.insert_many(mongo_notification_queue)
        mongo_notification_queue = []
        
    # 5. Bulk insert to MongoDB login_events
    if len(mongo_login_queue) >= 10000 or (force and mongo_login_queue):
        print(f"Inserting {len(mongo_login_queue)} login events into MongoDB...")
        mongo_login_col.insert_many(mongo_login_queue)
        mongo_login_queue = []
        
    # 6. Bulk insert to MongoDB device_events
    if len(mongo_device_queue) >= 10000 or (force and mongo_device_queue):
        print(f"Inserting {len(mongo_device_queue)} device events into MongoDB...")
        mongo_device_col.insert_many(mongo_device_queue)
        mongo_device_queue = []

def process_user_dataframe(user_id, df_user):
    global global_tx_id_counter, total_users_processed
    if df_user.empty:
        return
        
    # Map Card index to card_id
    user_cards_map = {card_idx: card_id for (u_id, card_idx), card_id in cards_cache.items() if u_id == user_id}
    df_user["card_id"] = df_user["Card"].map(user_cards_map)
    
    # Drop rows where card_id is null
    df_user = df_user.dropna(subset=["card_id"])
    if df_user.empty:
        return
        
    df_user = df_user.copy()
    df_user["card_id"] = df_user["card_id"].astype(int)
    
    # Vectorized string conversion for transaction_date
    year_str = df_user["Year"].astype(str)
    month_str = df_user["Month"].astype(str).str.zfill(2)
    day_str = df_user["Day"].astype(str).str.zfill(2)
    time_str = df_user["Time"].astype(str)
    df_user["transaction_date"] = year_str + "-" + month_str + "-" + day_str + " " + time_str + ":00"
    
    # Vectorized currency cleaning
    amount_series = df_user["Amount"].astype(str).str.replace("$", "", regex=False).str.replace(",", "", regex=False).str.strip()
    df_user["amount"] = pd.to_numeric(amount_series, errors="coerce").fillna(0.0)
        
    # Vectorized Zip cleaning
    if "Zip" in df_user.columns:
        zip_series = df_user["Zip"].fillna("").astype(str).str.strip()
        zip_series = zip_series.apply(lambda x: x[:-2] if x.endswith(".0") else x)
        # Replace empty strings or "nan" before padding or replace padded forms
        zip_series = zip_series.str.zfill(5)
        df_user["zip_clean"] = zip_series.replace("00000", None).replace("00nan", None).replace("nan", None).replace("", None)
    else:
        df_user["zip_clean"] = None
        
    # Vectorized Errors cleaning
    if "Errors?" in df_user.columns:
        df_user["errors_clean"] = df_user["Errors?"].fillna("").astype(str).str.strip()
        df_user["errors_clean"] = df_user["errors_clean"].replace("", None).replace("nan", None)
    else:
        df_user["errors_clean"] = None
        
    # Sort chronologically by date
    df_user = df_user.sort_values(by="transaction_date")
    
    # Sample the 150 most recent transactions
    N = 150
    selected_df = df_user.tail(N).copy()
    
    # Assign sequential transaction IDs
    n_rows = len(selected_df)
    tx_ids = list(range(global_tx_id_counter + 1, global_tx_id_counter + n_rows + 1))
    global_tx_id_counter += n_rows
    selected_df["id"] = tx_ids
    
    # Append to PostgreSQL bulk insert queue
    pg_records = []
    for _, r in selected_df.iterrows():
        pg_records.append({
            "id": int(r["id"]),
            "card_id": int(r["card_id"]),
            "year": int(r["Year"]),
            "month": int(r["Month"]),
            "day": int(r["Day"]),
            "time": str(r["Time"]),
            "amount": float(r["amount"]),
            "use_chip": r["Use Chip"] if pd.notna(r["Use Chip"]) else None,
            "merchant_name": str(r["Merchant Name"]),
            "merchant_city": r["Merchant City"] if pd.notna(r["Merchant City"]) else None,
            "merchant_state": r["Merchant State"] if pd.notna(r["Merchant State"]) else None,
            "zip": r["zip_clean"],
            "mcc": int(r["MCC"]) if pd.notna(r["MCC"]) else None,
            "errors": r["errors_clean"],
            "is_fraud": str(r["Is Fraud?"]),
            "transaction_date": str(r["transaction_date"]),
            "description": f"Payment to {r['Merchant Name']}"[:255]
        })
    pg_insert_queue.extend(pg_records)
    
    mongo_tx_list = []
    for _, r in selected_df.tail(10).iterrows():
        mongo_tx_list.append({
            "transaction_id": int(r["id"]),
            "amount": float(r["amount"]),
            "merchant": str(r["Merchant Name"]),
            "date": str(r["transaction_date"]),
            "is_fraud": str(r["Is Fraud?"]),
            "errors": r["errors_clean"] if pd.notna(r["errors_clean"]) else ""
        })
        
    mongo_customer_updates.append(UpdateOne(
        {"_id": int(user_id)},
        {"$set": {"recent_transactions": mongo_tx_list}}
    ))
    
    # Generate login, device, fraud events and notifications
    for _, r in selected_df.iterrows():
        is_fraud_bool = str(r["Is Fraud?"]).strip().lower() == "yes"
        tx_id = int(r["id"])
        tx_time_str = str(r["transaction_date"])
        
        try:
            tx_dt = datetime.datetime.strptime(tx_time_str, "%Y-%m-%d %H:%M:%S")
            app_open_time = (tx_dt - datetime.timedelta(minutes=random.randint(4, 8))).isoformat()
            login_time = (tx_dt - datetime.timedelta(minutes=random.randint(1, 3))).isoformat()
        except Exception:
            app_open_time = tx_time_str
            login_time = tx_time_str
            
        ip_address = f"{random.randint(24, 223)}.{random.randint(10, 250)}.{random.randint(0, 254)}.{random.randint(1, 254)}"
        device_type = random.choice(["iOS", "Android", "Desktop"])
        os_name = "iOS" if device_type == "iOS" else ("Android" if device_type == "Android" else random.choice(["macOS", "Windows"]))
        app_version = random.choice(["3.4.1", "3.5.0", "4.0.1"])
        device_id = f"dev_{random.randint(10000, 99999)}"
        
        # 30% sample for correlated logins and device events
        if random.random() < 0.3:
            mongo_login_queue.append({
                "user_id": int(user_id),
                "timestamp": login_time,
                "ip_address": ip_address,
                "device_type": device_type,
                "status": "success",
                "event_source": "transaction_flow",
                "location": {
                    "city": r["Merchant City"] if pd.notna(r["Merchant City"]) else None,
                    "state": r["Merchant State"] if pd.notna(r["Merchant State"]) else None
                }
            })
            
            mongo_device_queue.append({
                "user_id": int(user_id),
                "device_id": device_id,
                "os": os_name,
                "app_version": app_version,
                "event_type": "app_open",
                "event_source": "transaction_flow",
                "location": {
                    "lat": None,
                    "lon": None
                },
                "timestamp": app_open_time
            })
            
        # Fraud events & notification warnings
        if is_fraud_bool:
            mongo_fraud_queue.append({
                "transaction_id": tx_id,
                "customer_id": int(user_id),
                "card_id": int(r["card_id"]),
                "amount": float(r["amount"]),
                "merchant": str(r["Merchant Name"]),
                "transaction_date": tx_time_str,
                "risk_score": random.randint(85, 100),
                "fraud_reason": random.choice(["Unusual Location", "High Amount", "Suspicious Merchant", "Velocity Limit Exceeded"]),
                "status": "flagged",
                "reported_at": datetime.datetime.now().isoformat()
            })
            
            mongo_notification_queue.append({
                "customer_id": int(user_id),
                "type": "SMS Alert",
                "channel": "sms",
                "message_body": f"Alert: Suspected fraud detected on card ending in XXXX. Amount: ${float(r['amount'])}. Merchant: {r['Merchant Name']}.",
                "timestamp": tx_time_str,
                "status": "sent"
            })
            
    total_users_processed += 1
    if total_users_processed % 100 == 0:
        print(f"Aggregated transaction and event data for {total_users_processed} users...")
        
    flush_queues()

# --- CSV READING FLOW ---
start_time = time.time()
print(f"Reading {csv_path} in chunks and processing user blocks...")

current_user_id = None
current_user_dfs = []
chunk_size = 500000

for chunk_idx, chunk in enumerate(pd.read_csv(csv_path, dtype={"Zip": str}, chunksize=chunk_size)):
    print(f"Read chunk {chunk_idx + 1} from CSV...")
    
    # Process group-by-user contiguously
    for user_id, group_df in chunk.groupby("User", sort=False):
        user_id = int(user_id)
        
        if current_user_id is None:
            current_user_id = user_id
            current_user_dfs = [group_df]
        elif user_id == current_user_id:
            current_user_dfs.append(group_df)
        else:
            # User ID changed, process the block
            df_complete = pd.concat(current_user_dfs, ignore_index=True)
            process_user_dataframe(current_user_id, df_complete)
            
            # Reset buffer
            current_user_id = user_id
            current_user_dfs = [group_df]

# Process the very last user block
if current_user_id is not None and current_user_dfs:
    df_complete = pd.concat(current_user_dfs, ignore_index=True)
    process_user_dataframe(current_user_id, df_complete)

# Force flush remaining rows in all queues
flush_queues(force=True)

# Synchronize PostgreSQL transactions_id_seq sequence to prevent duplicate key errors during live simulation
print("Synchronizing transactions_id_seq sequence in PostgreSQL...")
with pg_engine.begin() as conn:
    conn.execute(text("SELECT setval('transactions_id_seq', COALESCE((SELECT MAX(id) FROM transactions), 1))"))

elapsed_time = time.time() - start_time
print(f"Successfully finished ingestion in {elapsed_time:.2f} seconds!")
print(f"Total users processed: {total_users_processed}")
print(f"Total transactions inserted into PostgreSQL: {global_tx_id_counter}")

mongo_client.close()
