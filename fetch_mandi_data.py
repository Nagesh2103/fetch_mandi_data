# fetch_mandi_data.py
import os
import requests
import pandas as pd
import logging
import traceback
from pymongo import MongoClient, UpdateOne
from datetime import datetime, timedelta

# CONFIG
RESOURCE_ID = "9ef84268-d588-465a-a308-a864a43d0070"
# Set default to empty string if not found, to avoid breaking the script
API_KEY = os.getenv("DATA_GOV_API_KEY") or "" 
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "agriculture_db"
COLLECTION_NAME = "recent_crop_prices"
LIMIT = 499

# Filter Constants
COMMODITIES_TO_KEEP = ["Onion"]
DAYS_TO_KEEP = 20

# Logging to stdout so Render captures it
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def fetch_data():
    if not API_KEY:
        logging.error("DATA_GOV_API_KEY is not set in environment.")
        return []
    fields = "commodity,state,district,arrival_date,min_price,max_price,modal_price"
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

def process_records(records):
    df = pd.DataFrame(records)

    req_cols = ["arrival_date", "state", "district", "commodity", "min_price", "max_price", "modal_price"]
    # Ensure all required columns exist, fill missing ones with None
    for c in req_cols:
        if c not in df.columns:
            df[c] = None
            
    df = df[req_cols]
    
    # Filter 1: Filter the DataFrame for only 'Onion'
    initial_count = len(df)
    df = df[df["commodity"].isin(COMMODITIES_TO_KEEP)].copy()
    logging.info("Filtered data. Kept %d records out of %d for %s", len(df), initial_count, ", ".join(COMMODITIES_TO_KEEP))

    # Convert date and handle errors
    df["arrival_date"] = pd.to_datetime(df["arrival_date"], errors="coerce", dayfirst=True)
    df = df.dropna(subset=["arrival_date"]) # Remove rows where date could not be parsed

    # Filter 2: Filter for the last 20 days
    date_limit = datetime.now() - timedelta(days=DAYS_TO_KEEP)
    df = df[df["arrival_date"] >= date_limit].copy()
    logging.info("Further filtered for records in the last %d days. Final count: %d", DAYS_TO_KEEP, len(df))

    df = df.sort_values(by="arrival_date", ascending=False)
    return df

def store_mongo(df):
    if df.empty:
        logging.info("No records to store.")
        return
    if not MONGO_URI:
        logging.error("MONGO_URI not set; cannot store to DB.")
        return
    
    # 🚨 Ensure UpdateOne is available (Imported above)
    try:
        # Added serverSelectionTimeoutMS for robustness
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[DB_NAME]
        col = db[COLLECTION_NAME]
        
        df_mongo = df.copy()
        
        # Convert pandas datetime objects to native python datetime objects for PyMongo
        df_mongo["arrival_date"] = df_mongo["arrival_date"].apply(
            lambda x: None if pd.isnull(x) else x.to_pydatetime()
        )
        docs = df_mongo.to_dict(orient="records")
        
        # Prepare for bulk UPSERT operation
        requests = []
        for doc in docs:
            # 1. Define the unique key (query filter for upsert)
            query = {
                "commodity": doc.get("commodity"),
                "state": doc.get("state"),
                "district": doc.get("district"),
                "arrival_date": doc.get("arrival_date")
            }
            
            # 2. Define the replacement document: $set all fields
            # Note: We use $set to only replace fields provided, but since we provide all
            # original fields, this effectively updates everything based on the query.
            update_op = {"$set": doc}
            
            # 3. Create the UpdateOne request with upsert=True
            requests.append(UpdateOne(query, update_op, upsert=True))

        # Execute the bulk write operation
        if requests:
            result = col.bulk_write(requests, ordered=False)
            logging.info("Bulk Upsert successful: Upserted %d, Matched %d, Modified %d records.", 
                         result.upserted_count, result.matched_count, result.modified_count)
        
        # Log total count as requested in the old logic for context
        logging.info("Total documents in collection after update: %d", col.count_documents({}))
        
        client.close()
    except Exception as e:
        logging.error("MongoDB error: %s", str(e))
        # Log the full stack trace which is essential for remote debugging on Render
        logging.error(traceback.format_exc())

def main():
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
