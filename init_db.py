import logging
from sqlalchemy import create_engine, Column, Integer, String, Numeric, DateTime, ForeignKey, text, UniqueConstraint, BigInteger
from sqlalchemy.orm import declarative_base
from pymongo import MongoClient
import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- POSTGRESQL CONFIGURATION ---
PG_DATABASE_URL = "postgresql+psycopg://admin:admin@localhost:5434/banking_core"
engine = create_engine(PG_DATABASE_URL, echo=True)
Base = declarative_base()

# Define SQLAlchemy models representing ALL columns in CSV files
class Customer(Base):
    __tablename__ = "customers"
    
    id = Column(Integer, primary_key=True) # Mapped to CSV row index / User ID
    name = Column(String(100), nullable=False) # Person
    email = Column(String(100), unique=True, nullable=False) # Generated unique email
    phone = Column(String(20), nullable=True) # None
    current_age = Column(Integer) # Current Age
    retirement_age = Column(Integer) # Retirement Age
    birth_year = Column(Integer) # Birth Year
    birth_month = Column(Integer) # Birth Month
    gender = Column(String(20)) # Gender
    address = Column(String(255)) # Address
    apartment = Column(String(50), nullable=True) # Apartment
    city = Column(String(100)) # City
    state = Column(String(50)) # State
    zipcode = Column(String(20)) # Zipcode
    latitude = Column(Numeric(10, 6)) # Latitude
    longitude = Column(Numeric(10, 6)) # Longitude
    per_capita_income_zipcode = Column(Numeric(15, 2)) # Per Capita Income - Zipcode
    yearly_income = Column(Numeric(15, 2)) # Yearly Income - Person
    total_debt = Column(Numeric(15, 2)) # Total Debt
    fico_score = Column(Integer) # FICO Score
    num_credit_cards = Column(Integer) # Num Credit Cards
    created_at = Column(DateTime, server_default=text("CURRENT_TIMESTAMP"))

class Card(Base):
    __tablename__ = "cards"
    
    id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey("customers.id", ondelete="CASCADE"), nullable=False)
    card_index = Column(Integer, nullable=False) # CARD INDEX
    card_brand = Column(String(50)) # Card Brand
    card_type = Column(String(50)) # Card Type
    card_number = Column(String(50), unique=True, nullable=False) # Card Number
    expires = Column(String(20)) # Expires
    cvv = Column(String(10)) # CVV
    has_chip = Column(String(10)) # Has Chip
    cards_issued = Column(Integer) # Cards Issued
    credit_limit = Column(Numeric(15, 2)) # Credit Limit
    acct_open_date = Column(String(20)) # Acct Open Date
    year_pin_last_changed = Column(Integer) # Year PIN last Changed
    card_on_dark_web = Column(String(10)) # Card on Dark Web
    status = Column(String(20), default="active", nullable=False) # Derived status (active/blocked)
    created_at = Column(DateTime) # Derived timestamp from acct_open_date
    
    __table_args__ = (UniqueConstraint('customer_id', 'card_index', name='_customer_card_uc'),)

class Transaction(Base):
    __tablename__ = "transactions"
    
    id = Column(BigInteger, primary_key=True) # Primary Key (incremented during load)
    card_id = Column(Integer, ForeignKey("cards.id", ondelete="CASCADE"), nullable=False)
    year = Column(Integer) # Year
    month = Column(Integer) # Month
    day = Column(Integer) # Day
    time = Column(String(20)) # Time
    amount = Column(Numeric(15, 2), nullable=False) # Amount
    use_chip = Column(String(50)) # Use Chip
    merchant_name = Column(String(255)) # Merchant Name
    merchant_city = Column(String(100), nullable=True) # Merchant City
    merchant_state = Column(String(50), nullable=True) # Merchant State
    zip = Column(String(20), nullable=True) # Zip
    mcc = Column(Integer) # MCC
    errors = Column(String(255), nullable=True) # Errors?
    is_fraud = Column(String(10)) # Is Fraud?
    transaction_date = Column(DateTime, nullable=False) # Derived timestamp
    description = Column(String(255), nullable=True) # Merchant Name / Notes


# --- MONGODB CONFIGURATION ---
MONGO_URI = "mongodb://admin:admin@localhost:27017/?directConnection=true"
MONGO_DB_NAME = "banking_events"
MONGO_COLLECTIONS = [
    "customers",
    "login_events",
    "fraud_events",
    "audit_logs",
    "notification_logs",
    "device_events"
]


def init_postgres():
    logger.info("Initializing PostgreSQL schema...")
    try:
        # Drop existing tables to start clean
        Base.metadata.drop_all(bind=engine)
        # Create tables
        Base.metadata.create_all(bind=engine)
        logger.info("PostgreSQL tables ('customers', 'cards', 'transactions') created successfully.")
    except Exception as e:
        logger.error(f"Error creating PostgreSQL tables: {e}")
        raise e


def init_mongodb():
    logger.info("Initializing MongoDB database & collections...")
    try:
        client = MongoClient(MONGO_URI)
        # Drop the entire database to clear old data
        client.drop_database(MONGO_DB_NAME)
        db = client[MONGO_DB_NAME]
        
        for col_name in MONGO_COLLECTIONS:
            db.create_collection(col_name)
            logger.info(f"MongoDB collection '{col_name}' initialized.")
            
        client.close()
        logger.info("MongoDB database initialized successfully!")
    except Exception as e:
        logger.error(f"Error initializing MongoDB: {e}")
        raise e


if __name__ == "__main__":
    logger.info("Starting database initialization...")
    init_postgres()
    init_mongodb()
    logger.info("Database initialization completed successfully!")
