# main.py
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
import traceback
from predictor import get_live_forecast

app = FastAPI(
    title="Agricultural Price Forecast API",
    description="An API to get 7-day price forecasts for all districts."
)

class ForecastRequest(BaseModel):
    crop_name: str
    variety_name: str
    district_name: str # <-- Botpress MUST send this

@app.get("/test-files")
def test_files():
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

@app.post("/forecast")
async def get_forecast(request: ForecastRequest):
    try:
        print(f"Received request for: {request.district_name}, {request.crop_name}, {request.variety_name}")
        
        forecast_df = get_live_forecast(
            district_name=request.district_name,
            crop_name=request.crop_name, 
            variety_name=request.variety_name
        )
        
        if forecast_df is None:
            raise HTTPException(status_code=404, detail=f"Could not generate forecast. No model or recent data for {request.district_name}.")
            
        forecast_json = forecast_df.to_dict(orient="records")
        return {"forecast": forecast_json}
    except Exception as e:
        print(f"Error during forecast: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    # Directly provide the app object instead of string reference
    uvicorn.run(app, host="0.0.0.0", port=port, reload=False)



