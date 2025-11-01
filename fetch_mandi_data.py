# fetch_mandi_data.py
import os
import requests
import pandas as pd
import logging
import traceback
from pymongo import MongoClient, UpdateOne
from datetime import datetime, timedelta

# --- CONFIG ---
RESOURCE_ID = "9ef84268-d588-465a-a308-a864a43d0070"
API_KEY = os.getenv("DATA_GOV_API_KEY") or ""
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "agriculture_db"
COLLECTION_NAME = "recent_crop_prices"
LIMIT = 499

# Filter Constants
COMMODITIES_TO_KEEP = ["Onion"]
DAYS_TO_KEEP = 20 # Data for the previous 20 days

# ⭐ NEW STATE AND DISTRICT FILTERING CONSTANTS ⭐
TARGET_STATE = "Maharashtra"
TARGET_DISTRICTS = [
    "Ahmednagar", "Akola", "Amarawati", "Beed", "Buldhana", "Chandrapur",
    "Chattrapati Sambhajinagar", "Dharashiv(Usmanabad)", "Dhule", "Jalana", 
    "Jalgaon", "Kolhapur", "Latur", "Mumbai", "Nagpur", "Nandurbar", 
    "Nashik", "Pune", "Raigad", "Ratnagiri", "Sangli", "Satara", 
    "Sholapur", "Thane", "Wardha"
]

# Logging to stdout so Render captures it
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- DATA FETCHING (No Change Required Here) ---
def fetch_data():
    """Fetches the latest mandi price data from the data.gov.in API."""
    if not API_KEY:
        logging.error("DATA_GOV_API_KEY is not set in environment.")
        return []
        
    # Fields include commodity, variety, and market
    fields = "commodity,state,district,market,variety,arrival_date,min_price,max_price,modal_price"
    
    url = (
        f"https://api.data.gov.in/resource/{RESOURCE_ID}"
        f"?api-key={API_KEY}&format=json&offset=0&limit={LIMIT}&fields={fields}"
    )
    logging.info("Requesting URL: %s", url)
    
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        payload = r.json()
        records = payload.get("records", [])
        logging.info("Fetched %d records from API", len(records))
        return records
    except requests.exceptions.RequestException as e:
        logging.error("Request failed: %s", str(e))
        return []
    except Exception as e:
        logging.error("Unexpected error in fetch_data: %s", str(e))
        return []

# --- DATA PROCESSING (Filtering Logic Added Here) ---
def process_records(records):
    """
    Converts records to a DataFrame, filters for target commodities,
    CLEANS AND FILTERS FOR MAHARASHTRA DISTRICTS, and keeps only recent records.
    """
    df = pd.DataFrame(records)

    req_cols = ["arrival_date", "state", "district", "market", "commodity", "variety", "min_price", "max_price", "modal_price"]
    for c in req_cols:
        if c not in df.columns:
            df[c] = None
            
    df = df[req_cols]
    
    # 1. CLEANING: Ensure price columns are strictly numeric
    price_cols = ["min_price", "max_price", "modal_price"]
    for col in price_cols:
        # Convert to numeric, setting non-numeric values to NaN
        df[col] = pd.to_numeric(df[col], errors='coerce')
        
    # Drop rows where the primary price (modal_price) is NaN after coercion
    df = df.dropna(subset=['modal_price']).copy()
    
    # ⭐ FINAL FIX: Filter out non-positive prices to prevent log(0) errors
    initial_count_after_coerce = len(df)
    df = df[df['modal_price'] > 0].copy() 
    
    # Use f-string with formatting for clarity
    logging.info(f"Filtered out {initial_count_after_coerce - len(df)} records where modal_price was $\\le 0$.")
    
    # 2. Filter by Commodity
    initial_count = len(df)
    df = df[df["commodity"].isin(COMMODITIES_TO_KEEP)].copy()
    logging.info("Filtered data. Kept %d records out of %d for %s", len(df), initial_count, ", ".join(COMMODITIES_TO_KEEP))

    # ⭐ 3. FILTER BY STATE AND DISTRICT (NEW LOGIC) ⭐
    maharashtra_count = len(df)
    # State filter
    df = df[df["state"] == TARGET_STATE].copy()

    # District filter (case-insensitive for robustness)
    target_districts_lower = [d.lower() for d in TARGET_DISTRICTS]
    df = df[df["district"].str.lower().isin(target_districts_lower)].copy()
    
    logging.info("Filtered for %s. Kept %d records out of %d for target districts.", TARGET_STATE, len(df), maharashtra_count)

    # 4. Filter by Date (The 20-day logic)
    df["arrival_date"] = pd.to_datetime(df["arrival_date"], errors="coerce", dayfirst=True)
    df = df.dropna(subset=["arrival_date"])

    date_limit = datetime.now() - timedelta(days=DAYS_TO_KEEP)
    df = df[df["arrival_date"] >= date_limit].copy()
    logging.info("Further filtered for records in the last %d days. Final count: %d", DAYS_TO_KEEP, len(df))

    df = df.sort_values(by="arrival_date", ascending=False)
    return df

# --- DATABASE STORAGE (No Change Required Here) ---
def store_mongo(df):
    """
    Performs a bulk upsert into MongoDB using the 5 key fields as a unique identifier.
    """
    if df.empty:
        logging.info("No records to store.")
        return
    if not MONGO_URI:
        logging.error("MONGO_URI not set; cannot store to DB.")
        return
        
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[DB_NAME]
        col = db[COLLECTION_NAME]
        
        # Unique compound index includes commodity, variety, location, market, and date
        col.create_index([
            ("commodity", 1),
            ("variety", 1), 
            ("state", 1),
            ("district", 1),
            ("market", 1),   
            ("arrival_date", -1) 
        ], unique=True, name="unique_mandi_price_with_variety")
        logging.info("Ensured unique compound index exists.")
        
        df_mongo = df.copy()
        
        # Convert pandas datetime objects to native Python datetime objects for MongoDB
        df_mongo["arrival_date"] = df_mongo["arrival_date"].apply(
            lambda x: None if pd.isnull(x) else x.to_pydatetime()
        )
        docs = df_mongo.to_dict(orient="records")
        
        requests = []
        for doc in docs:
            # Define the unique key for matching/upserting
            query = {
                "commodity": doc.get("commodity"),
                "variety": doc.get("variety"),     
                "state": doc.get("state"),
                "district": doc.get("district"),
                "market": doc.get("market"),       
                "arrival_date": doc.get("arrival_date")
            }
            
            # The entire document is the update payload
            update_op = {"$set": doc}
            
            requests.append(UpdateOne(query, update_op, upsert=True))

        if requests:
            result = col.bulk_write(requests, ordered=False)
            logging.info("Bulk Upsert successful: Upserted %d, Matched %d, Modified %d records.", 
                          result.upserted_count, result.matched_count, result.modified_count)
            
        logging.info("Total documents in collection after update: %d", col.count_documents({}))
        
        client.close()
    except Exception as e:
        logging.error("MongoDB error: %s", str(e))
        logging.error(traceback.format_exc())

# --- MAIN EXECUTION (No Change Required Here) ---
def main():
    """Main function to run the data pipeline."""
    logging.info("Job started")
    records = fetch_data()
    if not records:
        logging.info("No records fetched this run.")
        return
        
    df = process_records(records)
    store_mongo(df)
    logging.info("Job finished")

if __name__ == "__main__":
    main()
