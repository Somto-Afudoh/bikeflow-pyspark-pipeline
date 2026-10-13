from pyspark.sql.functions import col, concat_ws, current_timestamp, date_trunc, lit, xxhash64

CATALOG = "optiridecatalog"


# select the batch id
batch_id = spark.sql(f"SELECT max(batch_id) FROM {CATALOG}.bronze.bike_raw").collect()[0][0]
print(f"batch_id = {batch_id}")

# For Dimension Station
# The idea in three sentences:
#   take this batch's description of every station
#   compare it with the version gold currently calls current
#   if anything differs, close the old row and insert a new one
#
# A "fingerprint" is just the attributes glued into one string. Comparing
# two strings is simpler to read than comparing six columns one by one.

snapshot = (
    spark.table(f"{CATALOG}.silver.station_status")
    .filter(col("batch_id") == batch_id)
    .select(
        "station_id",
        "station_name",
        "latitude",
        "longitude",
        "address",
        "total_slots",
        "reading_ts",
    )
    .dropDuplicates(["station_id"])
    .withColumn(
        "fingerprint",
        concat_ws("|", "station_name", "latitude", "longitude", "total_slots"),
    )
)

current_version = (
    spark.table(f"{CATALOG}.gold.dim_station")
    .filter(col("is_current") == True)
    .select(
        col("station_id").alias("current_station_id"),
        concat_ws("|", "station_name", "latitude", "longitude", "total_slots").alias(
            "current_fingerprint"
        ),
    )
)

# keep only stations that are brand new, or whose details changed
changed = (
    snapshot.join(
        current_version, snapshot.station_id == current_version.current_station_id, "left"
    )
    .filter(
        col("current_fingerprint").isNull()
        | (col("fingerprint") != col("current_fingerprint"))
    )
)

new_versions = changed.select(
    # the key of this VERSION: station plus the moment it became true
    xxhash64(concat_ws("|", col("station_id"), col("reading_ts").cast("string"))).alias(
        "station_key"
    ),
    "station_id",
    "station_name",
    "latitude",
    "longitude",
    "address",
    "total_slots",
    col("reading_ts").alias("valid_from"),
    lit(None).cast("timestamp").alias("valid_to"),
    lit(True).alias("is_current"),
)

new_versions.createOrReplaceTempView("new_versions")

# 2a. close the row that was current, if there was one
spark.sql(f"""
MERGE INTO {CATALOG}.gold.dim_station AS target
USING new_versions AS source
    ON target.station_id = source.station_id
   AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.valid_to   = source.valid_from,
    target.is_current = false
""")

# 2b. insert the new versions. Append, not merge: these rows are new by
#     definition, because their key includes the moment they started.
new_versions.write.mode("append").saveAsTable(f"{CATALOG}.gold.dim_station")

print("new station versions written:", new_versions.count())


# For Dimension weather Table
# Open-Meteo revises its forecast, so the same hour looks different in a
# later run. We keep both.
#
#   weather_id   the hour itself, shared by all versions of that hour
#   weather_key  the hour as captured in this batch, unique per row
#
# Insert only, never update: a forecast we already captured is history
# and must not be rewritten.

weather_new = (
    spark.table(f"{CATALOG}.silver.weather_hourly")
    .filter(col("batch_id") == batch_id)
    .withColumn("weather_id", xxhash64(col("weather_ts").cast("string")))
    .withColumn(
        "weather_key",
        xxhash64(concat_ws("|", col("weather_ts").cast("string"), col("batch_id"))),
    )
    .select(
        "weather_key",
        "weather_id",
        "weather_ts",
        "temperature_c",
        "precipitation_mm",
        "wind_speed_kmh",
        "cloud_cover",
        "humidity",
        "batch_id",
        col("ingest_at").alias("captured_at"),
    )
    .dropDuplicates(["weather_key"])
)

weather_new.createOrReplaceTempView("weather_new")

spark.sql(f"""
MERGE INTO {CATALOG}.gold.dim_weather AS target
USING weather_new AS source
    ON target.weather_key = source.weather_key
WHEN NOT MATCHED THEN INSERT *
""")

print("weather rows written:", weather_new.count())

# For the fact tables
# Each reading is joined to:
#   the weather for its hour, as captured in THIS batch
#   the station version that is current right now
#
# A station reports at 10:37 and the weather is recorded for 10:00, so we
# cut the reading time down to its hour before joining. Without that, the
# join matches nothing and weather_key comes out null.

readings = (
    spark.table(f"{CATALOG}.silver.station_status")
    .filter(col("batch_id") == batch_id)
    .withColumn("weather_hour", date_trunc("hour", col("reading_ts")))
)

weather_this_batch = (
    spark.table(f"{CATALOG}.gold.dim_weather")
    .filter(col("batch_id") == batch_id)
    .select("weather_key", "weather_id", "weather_ts")
)

station_now = (
    spark.table(f"{CATALOG}.gold.dim_station")
    .filter(col("is_current") == True)
    .select("station_key", col("station_id").alias("dim_station_id"))
)

fact_new = (
    readings
    .join(weather_this_batch, readings.weather_hour == weather_this_batch.weather_ts, "left")
    .join(station_now, readings.station_id == station_now.dim_station_id, "left")
    .withColumn(
        "fact_id",
        xxhash64(concat_ws("|", col("station_id"), col("reading_ts").cast("string"))),
    ).select(
        "fact_id",
        "station_key",
        "station_id",
        "weather_key",
        "weather_id",
        "reading_ts",
        "free_bikes",
        "available_slots",
        "total_slots",
        "utilisation_rate",
        "has_ebikes",
        "batch_id",
        "ingest_at",
    )
    .dropDuplicates(["fact_id"])
)

fact_new.createOrReplaceTempView("fact_new")

# Insert only. A reading we already recorded is not rewritten, so the
# history of what the network looked like remains the same.
spark.sql(f"""
MERGE INTO {CATALOG}.gold.fact_bike_weather AS target
USING fact_new AS source
    ON target.fact_id = source.fact_id
WHEN NOT MATCHED THEN INSERT *
""")

print("fact rows offered this run:", fact_new.count())


# To check yout tables

print("\nrow counts")
for table in ["dim_station", "dim_weather", "fact_bike_weather"]:
    count = spark.table(f"{CATALOG}.gold.{table}").count()
    print(f"  {table}: {count}")

# stations that have more than one version, and how many
display(spark.sql(f"""
    SELECT station_id, station_name, count(*) AS versions
    FROM {CATALOG}.gold.dim_station
    GROUP BY station_id, station_name
    HAVING count(*) > 1
    ORDER BY versions DESC
    LIMIT 10
"""))

# how many times each hour of weather has been captured
display(spark.sql(f"""
    SELECT weather_ts, count(*) AS captures, min(captured_at) AS first_seen, max(captured_at) AS last_seen
    FROM {CATALOG}.gold.dim_weather
    GROUP BY weather_ts
    ORDER BY captures DESC
    LIMIT 10
"""))