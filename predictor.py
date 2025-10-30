# predictor.py
# This file connects to MongoDB and uses the trained models.

import pandas as pd
import numpy as np
import joblib  # For loading models
from pymongo import MongoClient
import os

# --- 1. CONFIGURATION ---
# (Get details from your teammate's file)
MONGO_URI = os.getenv("MONGO_URI") # Or hardcode your string for testing
DB_NAME = "agriculture_db"
COLLECTION_NAME = "recent_crop_prices"

# This path will be where the models are in the FINAL project
# (e.g., a 'models' folder in the project)
MODEL_DIR = "./models/" 

# --- 2. CONNECT TO MONGODB ---
try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    print("Successfully connected to MongoDB.")
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")
    
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
        "variety": variety_name
    }
    # Find data, sort by date descending, and get the last 10 (to be safe)
    # Your teammate's file uses 'arrival_date'
    cursor = collection.find(query).sort("arrival_date", -1).limit(10)
    recent_data = pd.DataFrame(list(cursor))
    
    if recent_data.empty or len(recent_data) < 7:
        print(f"Error: Not enough recent data in MongoDB (found {len(recent_data)} records).")
        return None

    # --- C: Prepare Data for Prediction ---
    # (Must match the training data format)
    recent_data = recent_data.rename(columns={'arrival_date': 'ds', 'modal_price': 'y'})
    recent_data['ds'] = pd.to_datetime(recent_data['ds'])
    recent_data = recent_data.sort_values(by='ds').reset_index(drop=True) # Sort ascending
    
    # Log-transform all features
    recent_data['y'] = np.log1p(recent_data['y'])
    recent_data['min_price'] = np.log1p(recent_data['min_price'])
    recent_data['max_price'] = np.log1p(recent_data['max_price'])
    recent_data['Yesterday Price'] = recent_data['y'].shift(1)
    
    # Get the last 7 days to start the forecast
    future_df = recent_data.iloc[-7:].copy()
    future_df['Yesterday Price'] = future_df['Yesterday Price'].bfill() # Fill the one NaN

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

# --- 4. EXAMPLE OF HOW THE CHATBOT USES THIS ---
# (This part would be in your main app.py or rasa_actions.py)

# if __name__ == "__main__":
#     print("Running a test forecast for 'Onion' - 'Other'")
#     
#     # This assumes your teammate's script has run and MongoDB is populated
#     forecast = get_live_forecast(crop_name='Onion', variety_name='Other')
#     
#     if forecast is not None:
#         print("\nFinal 7-Day Forecast (in Rupees):")
#         print(forecast)