import time
import random
import logging
from datetime import datetime
from sqlalchemy import create_engine, select, text
from pymongo import MongoClient

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Databases URLs
PG_DATABASE_URL = "postgresql+psycopg://admin:admin@localhost:5434/banking_core"
MONGO_URI = "mongodb://admin:admin@localhost:27017/?directConnection=true"
MONGO_DB_NAME = "banking_events"

def main():
    logger.info("Connecting to databases...")
    engine = create_engine(PG_DATABASE_URL)
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB_NAME]
    
    # Check if cards exist in postgres
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, customer_id FROM cards LIMIT 100"))
        cards = [{"id": row[0], "customer_id": row[1]} for row in result]
        
    if not cards:
        logger.error("No cards found in PostgreSQL cards table. Run data import scripts first!")
        return

    # Mock parameters
    merchants = [
        {"name": "Walmart", "city": "Dallas", "state": "TX", "zip": "75201"},
        {"name": "Apple Store", "city": "Cupertino", "state": "CA", "zip": "95014"},
        {"name": "Starbucks", "city": "Seattle", "state": "WA", "zip": "98101"},
        {"name": "Amazon Online", "city": "Seattle", "state": "WA", "zip": "98109"},
        {"name": "Target", "city": "Minneapolis", "state": "MN", "zip": "55401"},
        {"name": "Shell Gas Station", "city": "Houston", "state": "TX", "zip": "77002"},
        {"name": "McDonalds", "city": "Chicago", "state": "IL", "zip": "60601"}
    ]
    
    user_agents = [
        {"os": "Android", "version": "13.0"},
        {"os": "iOS", "version": "16.5"},
        {"os": "Windows", "version": "11.0"},
        {"os": "macOS", "version": "13.4"}
    ]
    
    logger.info("Generating 10 live simulated transactions and events...")
    
    for i in range(10):
        # Pick a random card
        card = random.choice(cards)
        card_id = card["id"]
        customer_id = card["customer_id"]
        
        # 1. Generate Postgres Transaction
        merchant = random.choice(merchants)
        amount = round(random.uniform(5.0, 1500.0), 2)
        is_fraud = "Yes" if amount > 900.0 or random.random() < 0.05 else "No"
        now = datetime.now()
        
        tx_date_str = now.strftime("%Y-%m-%d %H:%M:%S")
        tx_time_str = now.strftime("%H:%M")
        
        # Insert transaction
        with engine.connect() as conn:
            # Get max ID or let Postgres handle it (it's Serial/BigInteger)
            res = conn.execute(text(
                """
                INSERT INTO transactions (card_id, year, month, day, time, amount, use_chip, merchant_name, merchant_city, merchant_state, zip, mcc, errors, is_fraud, transaction_date)
                VALUES (:card_id, :year, :month, :day, :time, :amount, :use_chip, :merchant_name, :merchant_city, :merchant_state, :zip, :mcc, :errors, :is_fraud, :transaction_date)
                RETURNING id
                """
            ), {
                "card_id": card_id,
                "year": now.year,
                "month": now.month,
                "day": now.day,
                "time": tx_time_str,
                "amount": amount,
                "use_chip": random.choice(["Swipe Transaction", "Chip Transaction", "Online Transaction"]),
                "merchant_name": merchant["name"],
                "merchant_city": merchant["city"],
                "merchant_state": merchant["state"],
                "zip": merchant["zip"],
                "mcc": random.choice([5411, 5812, 5814, 5311, 4812]),
                "errors": None if random.random() > 0.1 else "Insufficient Funds",
                "is_fraud": is_fraud,
                "transaction_date": now
            })
            tx_id = res.scalar()
            conn.commit()
            
        logger.info(f"Created live Postgres transaction ID {tx_id} for customer {customer_id} (amount: ${amount}, is_fraud: {is_fraud})")
        
        # 2. Generate MongoDB Device & Login Events
        ua = random.choice(user_agents)
        login_status = "Success" if random.random() > 0.08 else "Failed"
        device_type = "iOS" if ua["os"] == "iOS" else ("Android" if ua["os"] == "Android" else "Desktop")
        event_source = "MobileApp" if ua["os"] in ["Android", "iOS"] else "WebBrowser"
        
        # Device event
        mongo_db["device_events"].insert_one({
            "user_id": customer_id,
            "device_id": f"dev_{customer_id}_{random.randint(1000, 9999)}",
            "os": ua["os"],
            "app_version": ua["version"],
            "location": {
                "lat": None,
                "lon": None
            },
            "event_type": "LoginAttempt",
            "event_source": event_source,
            "timestamp": now.isoformat()
        })
        
        # Login event
        mongo_db["login_events"].insert_one({
            "user_id": customer_id,
            "status": login_status,
            "ip_address": f"192.168.1.{random.randint(2, 254)}",
            "device_type": device_type,
            "event_source": event_source,
            "location": {
                "city": merchant["city"],
                "state": merchant["state"]
            },
            "timestamp": now.isoformat()
        })
        
        # 3. Handle Fraud & Notifications if applicable
        if is_fraud == "Yes":
            # MongoDB Fraud Event
            mongo_db["fraud_events"].insert_one({
                "transaction_id": tx_id,
                "customer_id": customer_id,
                "card_id": card_id,
                "amount": amount,
                "merchant": merchant["name"],
                "transaction_date": tx_date_str,
                "risk_score": int(round(random.uniform(75.0, 99.9))),
                "fraud_reason": "High transaction amount or unusual location",
                "status": "Flagged",
                "reported_at": now.isoformat()
            })
            
            # MongoDB Notification Log
            channel = "SMS" if random.random() > 0.5 else "Email"
            mongo_db["notification_logs"].insert_one({
                "customer_id": customer_id,
                "type": "FraudAlert",
                "channel": channel,
                "message_body": f"Urgent: A transaction of ${amount} at {merchant['name']} was flagged as suspicious.",
                "status": "Sent",
                "timestamp": now.isoformat()
            })
            
        # Small delay to ensure separate timestamps
        time.sleep(0.5)

    mongo_client.close()
    logger.info("Live data generation finished successfully!")

if __name__ == "__main__":
    main()
