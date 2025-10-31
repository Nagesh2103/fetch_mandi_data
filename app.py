# main.py
# This file creates the API that Botpress will call.

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os

# Import the predictor function you already built
from predictor import get_live_forecast

# Initialize the FastAPI app
app = FastAPI(
    title="Agricultural Price Forecast API",
    description="An API to get 7-day price forecasts for crops."
)

# Define the input format (for error checking)
class ForecastRequest(BaseModel):
    crop_name: str
    variety_name: str

@app.get("/test-files")
def test_files():
    """
    A debug endpoint to see the file structure on the server.
    """
    root_path = "./"
    models_path = "./models"
    
    root_files = os.listdir(root_path)
    
    models_files = []
    if os.path.exists(models_path):
        models_files = os.listdir(models_path)
    else:
        models_files = [f"ERROR: '{models_path}' folder does not exist!"]
    
    return {
        "files_at_root (./)": root_files,
        "files_in_models_folder (./models)": models_files
    }
@app.get("/")
def read_root():
    return {"status": "Forecast API is running."}

# Define the main forecast endpoint
@app.post("/forecast")
async def get_forecast(request: ForecastRequest):
    """
    Runs the forecast model and returns the 7-day prediction.
    """
    try:
        print(f"Received forecast request for: {request.crop_name} - {request.variety_name}")
        
        # This is the line that calls your code!
        forecast_df = get_live_forecast(
            crop_name=request.crop_name, 
            variety_name=request.variety_name
        )
        
        if forecast_df is None:
            raise HTTPException(status_code=404, detail="Could not generate forecast. Data may be missing.")
            
        # Convert the DataFrame to a JSON format that Botpress can read
        forecast_json = forecast_df.to_dict(orient="records")
        
        return {"forecast": forecast_json}

    except Exception as e:
        print(f"Error during forecast: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# This allows the script to be run directly for testing
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))

    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
