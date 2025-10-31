import os
import requests
import pandas as pd
import logging
import traceback
from pymongo import MongoClient
from datetime import datetime, timedelta

# CONFIG
RESOURCE_ID = "9ef84268-d588-465a-a308-a864a43d0070"
API_KEY = os.getenv("DATA_GOV_API_KEY") or ""
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "agriculture_db"
COLLECTION_NAME = "recent_crop_prices"
# Set a higher limit to ensure enough data is fetched to cover the last 20 days
LIMIT = 499


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
        logging.error("Unexpected error: %s", str(e))
        return []

def process_records(records):
    df = pd.DataFrame(records)
    
    # 🚨 MODIFICATION 1: Filter for only "Onion"
    COMMODITIES_TO_KEEP = ["Onion"]
    DAYS_TO_KEEP = 20

    req_cols = ["arrival_date", "state", "district", "commodity", "min_price", "max_price", "modal_price"]
    for c in req_cols:
        if c not in df.columns:
            df[c] = None
            
    df = df[req_cols]
    
    # Filter the DataFrame for only 'Onion'
    initial_count = len(df)
    df = df[df["commodity"].isin(COMMODITIES_TO_KEEP)]
    logging.info("Filtered data. Kept %d records out of %d for %s", len(df), initial_count, ", ".join(COMMODITIES_TO_KEEP))

    df["arrival_date"] = pd.to_datetime(df["arrival_date"], errors="coerce", dayfirst=True)
    df = df.dropna(subset=["arrival_date"]) # Remove rows where date could not be parsed

    # 🚨 MODIFICATION 2: Filter for the last 20 days
    date_limit = datetime.now() - timedelta(days=DAYS_TO_KEEP)
    df = df[df["arrival_date"] >= date_limit]
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
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[DB_NAME]
        col = db[COLLECTION_NAME]
        
        # convert NaT -> None and datetimes -> python datetimes
        df_mongo = df.copy()
        df_mongo["arrival_date"] = df_mongo["arrival_date"].apply(lambda x: None if pd.isnull(x) else x.to_pydatetime())
        docs = df_mongo.to_dict(orient="records")
        
        # 🚨 MODIFICATION 3: Use upsert to insert new or replace existing documents (no delete_many)
        inserted_count = 0
        for doc in docs:
            # Create a unique key for upsert: commodity, state, district, and arrival_date
            query = {
                "commodity": doc.get("commodity"),
                "state": doc.get("state"),
                "district": doc.get("district"),
                "arrival_date": doc.get("arrival_date")
            }
            # The _id field in the replacement document should be removed or it will raise an error
            replacement_doc = {k: v for k, v in doc.items() if k != "_id"}
            
            # Replace the document if the query matches, or insert a new one (upsert=True)
            result = col.replace_one(query, replacement_doc, upsert=True)
            if result.upserted_id or result.modified_count == 1:
                inserted_count += 1
                
        logging.info("Processed %d documents for upsert. Total documents in DB now: %d", len(docs), col.count_documents({}))
        client.close()
    except Exception as e:
        logging.error("MongoDB error: %s", str(e))
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
