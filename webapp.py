from flask import Flask, jsonify
import os
import pymongo
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    logging.error("MONGO_URI not set in environment!")
    # app will still start but DB endpoints may fail

client = None
try:
    if MONGO_URI:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # Try to fetch server info to confirm connection
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

    docs = list(col.find({}, {"_id": 0})
                .sort("arrival_date", -1)
                .limit(200))

    return jsonify(docs)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
