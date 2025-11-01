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
DAYS_TO_KEEP = 20

# ⭐ STATE AND DISTRICT FILTERING CONSTANTS ⭐
# Note: Filters are applied as case-insensitive to match common API inconsistencies.
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


# --- DATA FETCHING ---
def fetch_data():
    """Fetches the latest mandi price data from the data.gov.in API."""
    if not API_KEY:
        logging.error("DATA_GOV_API_KEY is not set in environment.")
        return []

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


# --- DATA PROCESSING ---
def process_records(records):
    """
    Converts records to a DataFrame, filters for target commodities,
    cleans and filters for Maharashtra districts, and keeps only recent records.
    """
    if not records:
        return pd.DataFrame() # Return empty DataFrame if no records fetched
        
    df = pd.DataFrame(records)

    req_cols = [
        "arrival_date", "state", "district", "market",
        "commodity", "variety", "min_price", "max_price", "modal_price"
    ]
    for c in req_cols:
        if c not in df.columns:
            df[c] = None

    df = df[req_cols]

    # 1. CLEANING: Ensure price columns are numeric
    price_cols = ["min_price", "max_price", "modal_price"]
    for col in price_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Drop rows with NaN or non-positive modal_price
    df = df.dropna(subset=['modal_price']).copy()
    initial_count_after_coerce = len(df)
    df = df[df['modal_price'] > 0].copy()
    logging.info(f"Filtered out {initial_count_after_coerce - len(df)} records where modal_price <= 0.")

    # 2. Filter by Commodity
    initial_count = len(df)
    # Ensure commodity column is treated as string for case-insensitive comparison
    df["commodity"] = df["commodity"].astype(str)
    df = df[df["commodity"].str.lower().isin([c.lower() for c in COMMODITIES_TO_KEEP])].copy()
    logging.info("Filtered data. Kept %d records out of %d for %s", len(df), initial_count, ", ".join(COMMODITIES_TO_KEEP))

    # 3. FILTER BY STATE AND DISTRICT (CASE-INSENSITIVE)
    maharashtra_count = len(df)
    
    # State filter (Case-insensitive)
    df["state"] = df["state"].astype(str)
    df = df[df["state"].str.lower() == TARGET_STATE.lower()].copy()

    # District filter (Case-insensitive)
    target_districts_lower = [d.lower() for d in TARGET_DISTRICTS]
    df["district"] = df["district"].astype(str)
    df = df[df["district"].str.lower().isin(target_districts_lower)].copy()

    logging.info("Filtered for %s. Kept %d records out of %d for target districts.", TARGET_STATE, len(df), maharashtra_count)

    # 4. Filter by Date
    df["arrival_date"] = pd.to_datetime(df["arrival_date"], errors="coerce", dayfirst=True)
    df = df.dropna(subset=["arrival_date"])

    date_limit = datetime.now() - timedelta(days=DAYS_TO_KEEP)
    df = df[df["arrival_date"] >= date_limit].copy()
    logging.info("Further filtered for records in the last %d days. Final count: %d", DAYS_TO_KEEP, len(df))

    df = df.sort_values(by="arrival_date", ascending=False)
    return df


# --- DATABASE STORAGE ---
def store_mongo(df):
    """
    Performs a bulk upsert into MongoDB using compound keys as unique identifiers.
    """
    if df.empty:
        logging.info("No records to store.")
        return

    if not MONGO_URI:
        logging.error("MONGO_URI not set; cannot store to DB.")
        return

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # --- CONNECTION CHECK ---
        client.admin.command('ping')
        logging.info("Successfully pinged MongoDB server.")
        
        db = client[DB_NAME]
        col = db[COLLECTION_NAME]

        # Ensure index exists for efficient upserts
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
        df_mongo["arrival_date"] = df_mongo["arrival_date"].apply(
            lambda x: None if pd.isnull(x) else x.to_pydatetime()
        )

        docs = df_mongo.to_dict(orient="records")

        bulk_requests = []
        for doc in docs:
            query = {
                "commodity": doc.get("commodity"),
                "variety": doc.get("variety"),
                "state": doc.get("state"),
                "district": doc.get("district"),
                "market": doc.get("market"),
                "arrival_date": doc.get("arrival_date")
            }
            update_op = {"$set": doc}
            bulk_requests.append(UpdateOne(query, update_op, upsert=True))

        if bulk_requests:
            result = col.bulk_write(bulk_requests, ordered=False)
            logging.info(
                "Bulk Upsert successful: Upserted %d, Matched %d, Modified %d records.",
                result.upserted_count, result.matched_count, result.modified_count
            )

        logging.info("Total documents in collection after update: %d", col.count_documents({}))
        client.close()

    except Exception as e:
        logging.error("MongoDB error: %s", str(e))
        logging.error(traceback.format_exc())


# --- MAIN EXECUTION ---
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
