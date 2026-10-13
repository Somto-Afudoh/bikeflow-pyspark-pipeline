import json
import os
from datetime import datetime, timezone
import requests
from pyspark.sql.functions import current_timestamp, lit

# define our catalog
CATALOG = 'optiridecatalog'
RAW_FOLDER = f"/Volumes/{CATALOG}/bronze/raw"

# define the API urls
BIKE_API = "https://api.citybik.es/v2/networks/citi-bike-nyc"
WEATHER_API = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=40.7143&longitude=-74.006"
    "&hourly=temperature_2m,precipitation,wind_speed_10m,cloudcover,relativehumidity_2m"
)

# create a batch id
batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
print(f"Batch ID: {batch_id}")

# Call the API
bike_data = requests.get(BIKE_API, timeout=15).json()
weather_data = requests.get(WEATHER_API, timeout=15).json()

print(f"Stations Received: {len(bike_data['network']['stations'])}")
print(f"Weather Received: {len(weather_data['hourly']['time'])}")

# save the raw files to the volume
# A volume path works like a normal folder, so plain open() is fine.

os.makedirs(f"{RAW_FOLDER}/bike", exist_ok=True)
os.makedirs(f"{RAW_FOLDER}/weather", exist_ok=True)

bike_file = f"{RAW_FOLDER}/bike/bike_{batch_id}.json"
weather_file = f"{RAW_FOLDER}/weather/weather_{batch_id}.json"

with open(bike_file, "w") as f:
    json.dump(bike_data, f)

with open(weather_file, "w") as f:
    json.dump(weather_data, f)

print("saved:", bike_file)
print("saved:", weather_file)

# Append it to the bronze schema
bike_df = spark.read.option("multiline", True).json(bike_file).withColumn("batch_id", lit(batch_id)).withColumn("ingest_at", current_timestamp())
bike_df.write.mode("append").option("mergeSchema", 'true').saveAsTable(f"{CATALOG}.bronze.bike_raw")
print("bronze table updated")

weather_df = spark.read.option("multiline", True).json(weather_file).withColumn("batch_id", lit(batch_id)).withColumn("ingest_at", current_timestamp())
weather_df.write.mode("append").option("mergeSchema", 'true').saveAsTable(f"{CATALOG}.bronze.weather_raw")

display(
    spark.sql(f"""
        SELECT batch_id, ingest_at
        FROM {CATALOG}.bronze.bike_raw
        ORDER BY ingest_at DESC
        LIMIT 5
    """)
)