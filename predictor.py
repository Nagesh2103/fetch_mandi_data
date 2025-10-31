# predictor.py
# This file connects to MongoDB and uses the trained models.

import pandas as pd
import numpy as np
import joblib  # For loading models
from pymongo import MongoClient
import os
import traceback  # <-- ADDED for detailed error logging

# --- 1. CONFIGURATION ---
MONGO_URI = os.getenv("MONGO_URI") # Get URI securely from environment
DB_NAME = "agriculture_db"
COLLECTION_NAME = "recent_crop_prices"
MODEL_DIR = "./models/" 

# --- 2. CONNECT TO MONGODB ---
# This block runs once when the application starts (imported by app.py)

# Check if the connection string is actually available
if not MONGO_URI:
    # Raise a fatal error if MONGO_URI is missing, preventing service start
    print("FATAL ERROR: MONGO_URI environment variable is not set.")
    raise ValueError("MONGO_URI not configured.")

try:
    # Increased timeout for robustness on cloud deployment (e.g., 10 seconds)
    # This prevents the application from stalling forever if the connection is slow.
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000) 
    
    # Test the connection immediately. This command will block up to the timeout.
    client.admin.command('ismaster')
    
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    print("Successfully connected to MongoDB.")
    
except Exception as e:
    # Log the full stack trace which is CRUCIAL for debugging deployment failures
    print(f"FATAL Error connecting to MongoDB: {e}")
    print(traceback.format_exc())
    # Re-raise a ConnectionError to signal startup failure cleanly
    raise ConnectionError(f"Failed to connect to MongoDB at startup: {e}")
    
# --- 3. THE PREDICTION FUNCTION ---
def get_live_forecast(crop_name, variety_name):
    """
    Loads a saved model, gets live data from MongoDB, and generates a 7-day forecast.
    """
    print(f"\n--- Generating 7-Day Forecast for: {crop_name} - {variety_name} ---")
    
    # --- A: Load the Pre-Trained Model ---
    model_filename = f"{MODEL_DIR}{crop_name.lower()}_{variety_name.lower()}_model.joblib"
    try:
        model = joblib.load(model_filename)
        print(f"Loaded model: {model_filename}")
    except FileNotFoundError:
        print(f"Error: Model file '{model_filename}' not found. Please train it first.")
        return None

    # --- B: Get Recent Data from MongoDB ---
    query = {
        "commodity": crop_name,
        # NOTE on 'variety': This field must exist and be populated in your MongoDB collection
        # (by fetch_mandi_data.py) for this query to work.
        "variety": variety_name 
    }
    # Find data, sort by date descending, and get the last 10 (to be safe)
    cursor = collection.find(query).sort("arrival_date", -1).limit(10)
    recent_data = pd.DataFrame(list(cursor))
    
    if recent_data.empty or len(recent_data) < 7:
        print(f"Error: Not enough recent data in MongoDB (found {len(recent_data)} records).")
        return None

# --- C: Prepare Data for Prediction ---
    recent_data = recent_data.rename(columns={'arrival_date': 'ds', 'modal_price': 'y'})
    recent_data['ds'] = pd.to_datetime(recent_data['ds'])
    recent_data = recent_data.sort_values(by='ds').reset_index(drop=True) 
    
    # --- THIS IS THE FIX ---
    # Convert price columns from string (from MongoDB) to numeric
    recent_data['y'] = pd.to_numeric(recent_data['y'], errors='coerce')
    recent_data['min_price'] = pd.to_numeric(recent_data['min_price'], errors='coerce')
    recent_data['max_price'] = pd.to_numeric(recent_data['max_price'], errors='coerce')
    
    # Drop any rows that failed conversion (e.g., had text)
    recent_data = recent_data.dropna(subset=['y', 'min_price', 'max_price'])
    # -----------------------

    # Log-transform all features
    recent_data['y'] = np.log1p(recent_data['y'])  # <--- This will now work
    recent_data['min_price'] = np.log1p(recent_data['min_price'])
    recent_data['max_price'] = np.log1p(recent_data['max_price'])

    recent_data['Yesterday Price'] = recent_data['y'].shift(1)
    
    # Get the last 7 days to start the forecast
    future_df = recent_data.iloc[-7:].copy()
    future_df['Yesterday Price'] = future_df['Yesterday Price'].bfill()

    # --- D: Run the Recursive Forecast ---
    for i in range(7):
        current_day_data = future_df.iloc[[i]]
        forecast_day = model.predict(current_day_data)
        predicted_y_log = forecast_day['yhat'].iloc[0]
        
        if i < 6:
            future_df.loc[future_df.index[i+1], 'Yesterday Price'] = predicted_y_log
            
    # --- E: Get Final Forecast ---
    final_forecast = model.predict(future_df)
    
    # Inverse transform (expm1) to get back to Rupees
    final_forecast['predicted_price'] = np.expm1(final_forecast['yhat'])
    final_forecast[['yhat_lower', 'yhat_upper']] = np.expm1(final_forecast[['yhat_lower', 'yhat_upper']])
    
    return final_forecast[['ds', 'predicted_price', 'yhat_lower', 'yhat_upper']]

