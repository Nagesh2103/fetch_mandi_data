# fetch_mandi_scraper.py
import os
import requests
import pandas as pd
import logging
import traceback
from bs4 import BeautifulSoup
from pymongo import MongoClient

# CONFIG
MONGO_URI = (
    "mongodb+srv://AgricultureDb:Nageshms2003"
    "@cluster0.nqrzb0c.mongodb.net/agriculture_db"
    "?retryWrites=true&w=majority&appName=Cluster0"
)
DB_NAME = "agriculture_db"
COLLECTION_NAME = "recent_crop_prices"
LIMIT = 100

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Example source (Agmarknet daily prices page)
SOURCE_URL = "https://agmarknet.gov.in/SearchCmmMkt.aspx"  # replace with exact mandi data listing page

def fetch_data():
    logging.info("Requesting URL: %s", SOURCE_URL)

    try:
        response = requests.get(SOURCE_URL, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        # Extract table (assuming first data table on page)
        tables = pd.read_html(str(soup))
        if not tables:
            logging.error("No tables found on the page.")
            return []

        df = tables[0]  # take first table
        logging.info("Fetched %d rows from web page", len(df))
        return df

    except requests.exceptions.RequestException as e:
        logging.error("Request failed: %s", str(e))
        return pd.DataFrame()
    except Exception as e:
        logging.error("Unexpected error: %s", str(e))
        return pd.DataFrame()

def process_records(df):
    req_cols = ["Arrival Date", "State", "District", "Commodity", "Min Price", "Max Price", "Modal Price"]
    rename_map = {
        "Arrival Date": "arrival_date",
        "State": "state",
        "District": "district",
        "Commodity": "commodity",
        "Min Price": "min_price",
        "Max Price": "max_price",
        "Modal Price": "modal_price",
    }

    # Keep only required columns
    df = df[[c for c in req_cols if c in df.columns]]
    df = df.rename(columns=rename_map)

    # Normalize
    df["arrival_date"] = pd.to_datetime(df["arrival_date"], errors="coerce", dayfirst=True)
    df = df.sort_values(by="arrival_date", ascending=False).head(LIMIT)

    return df

def store_mongo(df):
    if df.empty:
        logging.info("No records to store.")
        return

    try:
        logging.info("Using MongoDB URI: %s", MONGO_URI)
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        logging.info("Connected to MongoDB successfully.")

        db = client[DB_NAME]
        col = db[COLLECTION_NAME]

        df_mongo = df.copy()
        df_mongo["arrival_date"] = df_mongo["arrival_date"].apply(lambda x: None if pd.isnull(x) else x.to_pydatetime())
        docs = df_mongo.to_dict(orient="records")

        col.delete_many({})
        if docs:
            col.insert_many(docs)
            logging.info("Inserted %d records into MongoDB", len(docs))

        client.close()
    except Exception as e:
        logging.error("MongoDB error: %s", str(e))
        logging.error(traceback.format_exc())

def main():
    logging.info("Job started")
    df = fetch_data()
    if df.empty:
        logging.info("No records fetched this run.")
        return
    df = process_records(df)
    store_mongo(df)
    logging.info("Job finished")

if __name__ == "__main__":
    main()
