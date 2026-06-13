import os
import pandas as pd
from sqlalchemy import create_engine, text
from pymongo import MongoClient
import datetime

# Database connections
PG_URL = "postgresql+psycopg://admin:admin@localhost:5434/banking_core"
MONGO_URL = "mongodb://admin:admin@localhost:27017/?directConnection=true"

pg_engine = create_engine(PG_URL)
mongo_client = MongoClient(MONGO_URL)
mongo_db = mongo_client["banking_events"]
mongo_customers_col = mongo_db["customers"]
mongo_audit_col = mongo_db["audit_logs"]
mongo_notification_col = mongo_db["notification_logs"]

csv_path = "data/cards.csv"

# 1. Check if CSV exists
if not os.path.exists(csv_path):
    raise FileNotFoundError(f"File '{csv_path}' không tồn tại. Vui lòng đặt file CSV của bạn tại đây để import.")

# 2. Read CSV and Force sensitive fields to String (dtype)
print(f"Reading {csv_path}...")
df_cards = pd.read_csv(csv_path, dtype={"Card Number": str, "CVV": str})

print("Importing cards to PostgreSQL and MongoDB...")
count = 0

def clean_currency(val):
    if pd.isna(val):
        return None
    return float(str(val).replace("$", "").replace(",", "").strip())

def clean_cvv(val):
    if pd.isna(val):
        return None
    val_str = str(val).strip()
    if val_str.endswith(".0"):
        val_str = val_str[:-2]
    # Pad to 3 digits for CVV
    return val_str.zfill(3)

# Clean up MongoDB card events before starting
mongo_audit_col.delete_many({"action": "card_created"})
mongo_notification_col.delete_many({"type": "Security Notice"})
# Clear cards array in customers collection to avoid duplicates
mongo_customers_col.update_many({}, {"$set": {"cards": []}})

with pg_engine.begin() as conn:
    for idx, row in df_cards.iterrows():
        customer_id = int(row["User"])
        card_index = int(row["CARD INDEX"])
        card_number = str(row["Card Number"])
        card_brand = row["Card Brand"]
        card_type = row["Card Type"]
        expires = str(row["Expires"])
        cvv = clean_cvv(row.get("CVV"))
        has_chip = row["Has Chip"]
        cards_issued = int(row["Cards Issued"])
        credit_limit = clean_currency(row.get("Credit Limit"))
        acct_open_date = str(row["Acct Open Date"])
        year_pin_changed = int(row["Year PIN last Changed"]) if pd.notna(row["Year PIN last Changed"]) else None
        card_on_dark_web = row["Card on Dark Web"]
        
        # Determine status
        dark_web = str(card_on_dark_web).strip().lower()
        status = "blocked" if dark_web == "yes" else "active"
        
        # Parse acct open date (MM/YYYY) to YYYY-MM-DD
        try:
            parts = acct_open_date.split("/")
            if len(parts) == 2:
                month, year = parts
                created_at = f"{year}-{month}-01 00:00:00"
            else:
                created_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            created_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
        # Insert to Postgres
        sql = text("""
            INSERT INTO cards (
                customer_id, card_index, card_brand, card_type, card_number, expires, cvv,
                has_chip, cards_issued, credit_limit, acct_open_date, year_pin_last_changed,
                card_on_dark_web, status, created_at
            )
            VALUES (
                :customer_id, :card_index, :card_brand, :card_type, :card_number, :expires, :cvv,
                :has_chip, :cards_issued, :credit_limit, :acct_open_date, :year_pin_last_changed,
                :card_on_dark_web, :status, :created_at
            )
            ON CONFLICT (customer_id, card_index) DO UPDATE SET
                card_brand = EXCLUDED.card_brand,
                card_type = EXCLUDED.card_type,
                card_number = EXCLUDED.card_number,
                expires = EXCLUDED.expires,
                cvv = EXCLUDED.cvv,
                has_chip = EXCLUDED.has_chip,
                cards_issued = EXCLUDED.cards_issued,
                credit_limit = EXCLUDED.credit_limit,
                acct_open_date = EXCLUDED.acct_open_date,
                year_pin_last_changed = EXCLUDED.year_pin_last_changed,
                card_on_dark_web = EXCLUDED.card_on_dark_web,
                status = EXCLUDED.status,
                created_at = EXCLUDED.created_at;
        """)
        conn.execute(sql, {
            "customer_id": customer_id,
            "card_index": card_index,
            "card_brand": card_brand,
            "card_type": card_type,
            "card_number": card_number,
            "expires": expires,
            "cvv": cvv,
            "has_chip": has_chip,
            "cards_issued": cards_issued,
            "credit_limit": credit_limit,
            "acct_open_date": acct_open_date,
            "year_pin_last_changed": year_pin_changed,
            "card_on_dark_web": card_on_dark_web,
            "status": status,
            "created_at": created_at
        })
        
        # 1. Update the customer profile in MongoDB by pushing the card to its cards list (including all columns)
        mongo_customers_col.update_one(
            {"_id": customer_id},
            {"$push": {
                "cards": {
                    "card_index": card_index,
                    "card_brand": card_brand,
                    "card_type": card_type,
                    "card_number": "***" + card_number[-4:],
                    "expires": expires,
                    "cvv": cvv,
                    "has_chip": has_chip,
                    "cards_issued": cards_issued,
                    "credit_limit": credit_limit,
                    "acct_open_date": acct_open_date,
                    "year_pin_last_changed": year_pin_changed,
                    "card_on_dark_web": card_on_dark_web,
                    "status": status
                }
            }}
        )
        
        # 2. Emit card creation event to MongoDB 'audit_logs' collection
        audit_log = {
            "action": "card_created",
            "table": "cards",
            "record_id": f"{customer_id}_{card_index}",
            "timestamp": datetime.datetime.now().isoformat(),
            "performed_by": "system_importer",
            "details": f"Issued {card_brand} {card_type} card ending in {card_number[-4:]} for user {customer_id}"
        }
        mongo_audit_col.insert_one(audit_log)
        
        # 3. Emit card security notice to notification_logs if blocked
        if status == "blocked":
            notification = {
                "customer_id": customer_id,
                "type": "Security Notice",
                "channel": "email",
                "message_body": f"Dear Customer, your {card_brand} card ending in {card_number[-4:]} has been blocked because it was detected on the dark web.",
                "timestamp": datetime.datetime.now().isoformat(),
                "status": "sent"
            }
            mongo_notification_col.insert_one(notification)
        count += 1

print(f"Successfully imported {count} cards!")

# 3. Synchronize PostgreSQL customer created_at dates with earliest card open dates
print("Synchronizing PostgreSQL customer created_at dates with earliest card open dates...")
with pg_engine.begin() as conn:
    conn.execute(text("""
        UPDATE customers 
        SET created_at = (
            SELECT min(created_at) 
            FROM cards 
            WHERE cards.customer_id = customers.id
        )
        WHERE EXISTS (
            SELECT 1 FROM cards WHERE cards.customer_id = customers.id
        );
    """))

# 4. Synchronize MongoDB customer created_at dates from PostgreSQL
print("Synchronizing MongoDB customer created_at dates...")
with pg_engine.connect() as conn:
    res = conn.execute(text("SELECT id, created_at FROM customers WHERE created_at IS NOT NULL"))
    for row in res:
        cust_id = int(row[0])
        created_at_dt = row[1]
        mongo_customers_col.update_one(
            {"_id": cust_id},
            {"$set": {"created_at": created_at_dt.isoformat()}}
        )

print("Synchronization completed successfully!")
mongo_client.close()
