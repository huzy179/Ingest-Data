import os
import pandas as pd
from sqlalchemy import create_engine, text
from pymongo import MongoClient
from faker import Faker
import datetime
import random

# Database connections
PG_URL = "postgresql+psycopg://admin:admin@localhost:5434/banking_core"
MONGO_URL = "mongodb://admin:admin@localhost:27017/?directConnection=true"

pg_engine = create_engine(PG_URL)
mongo_client = MongoClient(MONGO_URL)
mongo_db = mongo_client["banking_events"]
mongo_customers_col = mongo_db["customers"]
mongo_audit_col = mongo_db["audit_logs"]
mongo_login_col = mongo_db["login_events"]
mongo_device_col = mongo_db["device_events"]

csv_path = "data/users.csv"

# 1. Check if CSV exists
if not os.path.exists(csv_path):
    raise FileNotFoundError(f"File '{csv_path}' không tồn tại. Vui lòng đặt file CSV của bạn tại đây để import.")

# 2. Read CSV and Force sensitive fields to String (dtype)
print(f"Reading {csv_path}...")
df_users = pd.read_csv(csv_path, dtype={"Zipcode": str, "Apartment": str})

print("Importing customers to PostgreSQL and MongoDB...")
fake = Faker()
count = 0

def clean_currency(val):
    if pd.isna(val):
        return None
    return float(str(val).replace("$", "").replace(",", "").strip())

# Clean helpers to fix data quality issues
def clean_zipcode(val):
    if pd.isna(val):
        return None
    val_str = str(val).strip()
    if val_str.endswith(".0"):
        val_str = val_str[:-2]
    # Pad to 5 digits for US zipcodes
    return val_str.zfill(5)

def clean_apartment(val):
    if pd.isna(val):
        return None
    val_str = str(val).strip()
    if val_str.endswith(".0"):
        val_str = val_str[:-2]
    return val_str

# Clean up MongoDB collections before starting
mongo_customers_col.delete_many({})
mongo_audit_col.delete_many({"action": "customer_created"})
mongo_login_col.delete_many({})
mongo_device_col.delete_many({})

with pg_engine.begin() as conn:
    for idx, row in df_users.iterrows():
        customer_id = int(idx)  # Row index is customer ID
        name = row["Person"]
        email = name.lower().strip().replace(" ", ".").replace("'", "") + f".{customer_id}@example.com"
        
        # Generate a clean US phone number format: XXX-XXX-XXXX
        phone = f"{random.randint(200, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
        
        # Clean currency values
        per_capita_income = clean_currency(row.get("Per Capita Income - Zipcode"))
        yearly_income = clean_currency(row.get("Yearly Income - Person"))
        total_debt = clean_currency(row.get("Total Debt"))
        
        # Clean zipcode and apartment fields
        zipcode = clean_zipcode(row.get("Zipcode"))
        apartment = clean_apartment(row.get("Apartment"))
        
        # Insert to Postgres
        sql = text("""
            INSERT INTO customers (
                id, name, email, phone, current_age, retirement_age, birth_year, birth_month,
                gender, address, apartment, city, state, zipcode, latitude, longitude,
                per_capita_income_zipcode, yearly_income, total_debt, fico_score, num_credit_cards
            )
            VALUES (
                :id, :name, :email, :phone, :current_age, :retirement_age, :birth_year, :birth_month,
                :gender, :address, :apartment, :city, :state, :zipcode, :latitude, :longitude,
                :per_capita_income_zipcode, :yearly_income, :total_debt, :fico_score, :num_credit_cards
            )
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                email = EXCLUDED.email,
                phone = EXCLUDED.phone,
                current_age = EXCLUDED.current_age,
                retirement_age = EXCLUDED.retirement_age,
                birth_year = EXCLUDED.birth_year,
                birth_month = EXCLUDED.birth_month,
                gender = EXCLUDED.gender,
                address = EXCLUDED.address,
                apartment = EXCLUDED.apartment,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                zipcode = EXCLUDED.zipcode,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                per_capita_income_zipcode = EXCLUDED.per_capita_income_zipcode,
                yearly_income = EXCLUDED.yearly_income,
                total_debt = EXCLUDED.total_debt,
                fico_score = EXCLUDED.fico_score,
                num_credit_cards = EXCLUDED.num_credit_cards;
        """)
        conn.execute(sql, {
            "id": customer_id,
            "name": name,
            "email": email,
            "phone": phone,
            "current_age": int(row["Current Age"]) if pd.notna(row["Current Age"]) else None,
            "retirement_age": int(row["Retirement Age"]) if pd.notna(row["Retirement Age"]) else None,
            "birth_year": int(row["Birth Year"]) if pd.notna(row["Birth Year"]) else None,
            "birth_month": int(row["Birth Month"]) if pd.notna(row["Birth Month"]) else None,
            "gender": row["Gender"] if pd.notna(row["Gender"]) else None,
            "address": row["Address"] if pd.notna(row["Address"]) else None,
            "apartment": apartment,
            "city": row["City"] if pd.notna(row["City"]) else None,
            "state": row["State"] if pd.notna(row["State"]) else None,
            "zipcode": zipcode,
            "latitude": float(row["Latitude"]) if pd.notna(row["Latitude"]) else None,
            "longitude": float(row["Longitude"]) if pd.notna(row["Longitude"]) else None,
            "per_capita_income_zipcode": per_capita_income,
            "yearly_income": yearly_income,
            "total_debt": total_debt,
            "fico_score": int(row["FICO Score"]) if pd.notna(row["FICO Score"]) else None,
            "num_credit_cards": int(row["Num Credit Cards"]) if pd.notna(row["Num Credit Cards"]) else None
        })
        
        # 1. Create a rich nested customer profile document in MongoDB 'customers' collection
        customer_doc = {
            "_id": customer_id,
            "name": name,
            "email": email,
            "phone": phone,
            "demographics": {
                "current_age": int(row["Current Age"]) if pd.notna(row["Current Age"]) else None,
                "retirement_age": int(row["Retirement Age"]) if pd.notna(row["Retirement Age"]) else None,
                "birth_year": int(row["Birth Year"]) if pd.notna(row["Birth Year"]) else None,
                "birth_month": int(row["Birth Month"]) if pd.notna(row["Birth Month"]) else None,
                "gender": row["Gender"] if pd.notna(row["Gender"]) else None,
                "address": row["Address"] if pd.notna(row["Address"]) else None,
                "apartment": apartment,
                "city": row["City"] if pd.notna(row["City"]) else None,
                "state": row["State"] if pd.notna(row["State"]) else None,
                "zipcode": zipcode,
                "location": {
                    "lat": float(row["Latitude"]) if pd.notna(row["Latitude"]) else None,
                    "lon": float(row["Longitude"]) if pd.notna(row["Longitude"]) else None
                }
            },
            "financial_profile": {
                "yearly_income": yearly_income,
                "total_debt": total_debt,
                "fico_score": int(row["FICO Score"]) if pd.notna(row["FICO Score"]) else None,
                "per_capita_income_zipcode": per_capita_income,
                "num_credit_cards": int(row["Num Credit Cards"]) if pd.notna(row["Num Credit Cards"]) else None
            },
            "cards": [], # Will be populated by import_cards.py
            "recent_transactions": [], # Will be populated by import_transactions.py
            "created_at": datetime.datetime.now().isoformat() # Will be corrected by import_cards.py
        }
        mongo_customers_col.insert_one(customer_doc)
        
        # 2. Emit audit log event to MongoDB 'audit_logs' collection
        audit_log = {
            "action": "customer_created",
            "table": "customers",
            "record_id": customer_id,
            "timestamp": datetime.datetime.now().isoformat(),
            "performed_by": "system_importer",
            "details": f"Customer profile created for {name} ({email})"
        }
        mongo_audit_col.insert_one(audit_log)
        
        # 3. Generate initial login and device events for each user
        for k in range(3):
            random_days = random.randint(100, 2000)
            login_time = (datetime.datetime.now() - datetime.timedelta(days=random_days, hours=random.randint(0, 23))).isoformat()
            
            ip_address = f"{random.randint(24, 223)}.{random.randint(10, 250)}.{random.randint(0, 254)}.{random.randint(1, 254)}"
            device_type = random.choice(["iOS", "Android", "Desktop", "Mobile Web"])
            os_name = "iOS" if device_type == "iOS" else ("Android" if device_type == "Android" else random.choice(["macOS", "Windows", "Linux"]))
            app_version = random.choice(["3.4.1", "3.5.0", "4.0.1"])
            device_id = f"dev_{random.randint(10000, 99999)}"
            
            mongo_login_col.insert_one({
                "user_id": customer_id,
                "timestamp": login_time,
                "ip_address": ip_address,
                "device_type": device_type,
                "status": random.choice(["success", "success", "success", "failed"]),
                "location": {
                    "city": row["City"] if pd.notna(row["City"]) else None,
                    "state": row["State"] if pd.notna(row["State"]) else None
                }
            })
            
            mongo_device_col.insert_one({
                "user_id": customer_id,
                "device_id": device_id,
                "os": os_name,
                "app_version": app_version,
                "event_type": random.choice(["app_open", "biometric_auth", "settings_changed"]),
                "location": {
                    "lat": float(row["Latitude"]) if pd.notna(row["Latitude"]) else None,
                    "lon": float(row["Longitude"]) if pd.notna(row["Longitude"]) else None
                },
                "timestamp": login_time
            })
        count += 1

print(f"Successfully imported {count} customers!")
mongo_client.close()
