# predictor.py
# FINAL CORRECT VERSION WITH FUTURE DATE FIX

import pandas as pd
import numpy as np
import joblib
from pymongo import MongoClient
import os
import traceback

# --- 1. CONFIGURATION ---
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "agriculture_db"
COLLECTION_NAME = "recent_crop_prices"
MODEL_DIR = "./models/"

# --- 2. CONNECT TO MONGODB ---
if not MONGO_URI:
    print("FATAL ERROR: MONGO_URI environment variable is not set.")
    raise ValueError("MONGO_URI not configured.")

try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    client.admin.command("ismaster")
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    print("Successfully connected to MongoDB.")
except Exception as e:
    print(f"FATAL Error connecting to MongoDB: {e}")
    print(traceback.format_exc())
    raise ConnectionError(f"Failed to connect to MongoDB at startup: {e}")


# --- 3. PREDICTION FUNCTION ---
def get_live_forecast(district_name, crop_name, variety_name):

    print(f"\n--- Forecast Request for: {district_name} - {crop_name} - {variety_name} ---")

    # --- A: Load Correct Model ---
    model_filename = f"{MODEL_DIR}{district_name.lower()}_{crop_name.lower()}_{variety_name.lower()}_model.joblib"

    try:
        model = joblib.load(model_filename)
        print(f"Loaded model: {model_filename}")
    except FileNotFoundError:
        print(f"Error: Model file '{model_filename}' not found.")
        print("This district/crop/variety is not supported yet.")
        return None

    # --- B: Pull The Latest MongoDB Record ---
    query = {
        "commodity": crop_name,
        "variety": variety_name,
        "district": district_name,
    }

    cursor = collection.find(query).sort("arrival_date", -1).limit(1)
    latest_records = list(cursor)

    if not latest_records:
        print(f"No recent data found for {crop_name} - {variety_name} in {district_name}.")
        return None

    latest_data = pd.DataFrame(latest_records)
    print(f"Latest record found: {latest_data['arrival_date'].iloc[0]}")

    # --- C: Prepare Data ---
    latest_data["ds"] = pd.to_datetime(latest_data["arrival_date"])

    latest_data["y"] = pd.to_numeric(latest_data["modal_price"], errors="coerce")
    latest_data["min_price"] = pd.to_numeric(latest_data["min_price"], errors="coerce")
    latest_data["max_price"] = pd.to_numeric(latest_data["max_price"], errors="coerce")

    latest_data = latest_data.dropna(subset=["y", "min_price", "max_price"])

    latest_data["y"] = np.log1p(latest_data["y"])
    latest_data["min_price"] = np.log1p(latest_data["min_price"])
    latest_data["max_price"] = np.log1p(latest_data["max_price"])

    last_known_y = latest_data["y"].iloc[0]
    last_known_min_price = latest_data["min_price"].iloc[0]
    last_known_max_price = latest_data["max_price"].iloc[0]

    # --- D: FIX — Generate Future Dates Based on the Latest MongoDB Date ---
    last_date = latest_data["ds"].iloc[0]
    future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=7)

    future_df = pd.DataFrame({"ds": future_dates})

    # --- E: Recursive Forecast Setup ---
    future_df["min_price"] = last_known_min_price
    future_df["max_price"] = last_known_max_price
    future_df["Yesterday Price"] = np.nan
    future_df.loc[future_df.index[0], "Yesterday Price"] = last_known_y

    print("Running recursive forecast for the next 7 days...")

    for i in range(7):
        row = future_df.iloc[[i]]
        forecast = model.predict(row)
        predicted_y_log = forecast["yhat"].iloc[0]

        if i < 6:
            future_df.loc[future_df.index[i + 1], "Yesterday Price"] = predicted_y_log

    # --- F: Final Forecast ---
    final_forecast = model.predict(future_df)

    final_forecast["predicted_price"] = np.expm1(final_forecast["yhat"])
    final_forecast["yhat_lower"] = np.expm1(final_forecast["yhat_lower"])
    final_forecast["yhat_upper"] = np.expm1(final_forecast["yhat_upper"])

    return final_forecast[["ds", "predicted_price", "yhat_lower", "yhat_upper"]]
