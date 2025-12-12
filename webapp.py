from flask import Flask, jsonify, request
import os
import pymongo
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    logging.error("MONGO_URI not set in environment! (DB endpoints may fail)")

client = None
try:
    if MONGO_URI:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        logging.info("Connected to MongoDB Atlas.")
except Exception as e:
    logging.error("Could not connect to MongoDB: %s", str(e))
    client = None

@app.route("/")
def home():
    return "🚜 AgriSwasthya: Mandi Data API is running!"

@app.route("/data")
def get_data():
    if not client:
        return jsonify({"error": "DB not connected"}), 500
    db = client["agriculture_db"]
    col = db["recent_crop_prices"]
    docs = list(col.find({}, {"_id": 0}).sort("arrival_date", -1).limit(200))
    return jsonify(docs)

# ---------------------------
# NEW: POST /forecast endpoint
# This accepts JSON with one of the payload shapes your Botpress action might send:
#   { "crop_name":"...", "variety_name":"...", "district_name":"..." }
# or alternatives like { "crop":"...", "variety":"...", "district":"..." }
# It returns JSON with "forecast": [ { ds, predicted_price, yhat_lower, yhat_upper }, ... ]
# ---------------------------
@app.route("/forecast", methods=["POST"])
def forecast():
    payload = request.get_json(force=True, silent=True) or {}
    app.logger.info("DEBUG POST /forecast payload=%s", payload)

    # Accept multiple key name styles
    crop = payload.get("crop_name") or payload.get("crop")
    variety = payload.get("variety_name") or payload.get("variety")
    district = payload.get("district_name") or payload.get("district")

    # If missing required fields, return helpful 400 so you don't get ambiguous 404/500
    if not (crop and variety and district):
        app.logger.warning("Missing fields in /forecast payload: crop=%s variety=%s district=%s", crop, variety, district)
        return jsonify({
            "detail": "Missing required fields. Please provide crop_name (or crop), variety_name (or variety), and district_name (or district).",
            "received": payload
        }), 400

    # TODO: Replace this sample forecast with your real model/DB lookup.
    sample_forecast = [
        {"ds": "2025-12-13", "predicted_price": 2000, "yhat_lower": 1800, "yhat_upper": 2200},
        {"ds": "2025-12-14", "predicted_price": 2050, "yhat_lower": 1850, "yhat_upper": 2250},
        {"ds": "2025-12-15", "predicted_price": 2075, "yhat_lower": 1875, "yhat_upper": 2275}
    ]

    return jsonify({"forecast": sample_forecast})

# Print registered routes at startup for debugging
def log_routes():
    try:
        for rule in app.url_map.iter_rules():
            app.logger.info("ROUTE: %s  METHODS: %s", rule.rule, ",".join(sorted(rule.methods)))
    except Exception as e:
        app.logger.warning("Could not print routes: %s", str(e))

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    log_routes()
    app.run(host="0.0.0.0", port=port)
