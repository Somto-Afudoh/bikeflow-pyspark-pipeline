CATALOG = 'optirideCatalog'
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")

# create the schemas in the optiride catalog
schema_list = ["bronze", "silver", "gold"]
for schema in schema_list:
  spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

# create the volume in the bronze layer
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.raw")

# create the dimension and fact tables in the gold layer

# dimension station table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.gold.dim_station (
    station_key  BIGINT,
    station_id   STRING,
    station_name STRING,
    latitude     DOUBLE,
    longitude    DOUBLE,
    address      STRING,
    total_slots  INT,
    valid_from   TIMESTAMP,
    valid_to     TIMESTAMP,
    is_current   BOOLEAN
)
""")

# dimension weather table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.gold.dim_weather (
    weather_key      BIGINT,
    weather_id       BIGINT,
    weather_ts       TIMESTAMP,
    temperature_c    DOUBLE,
    precipitation_mm DOUBLE,
    wind_speed_kmh   DOUBLE,
    cloud_cover      DOUBLE,
    humidity         DOUBLE,
    batch_id         STRING,
    captured_at      TIMESTAMP
)
""")

# fact Bike and Weather table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.gold.fact_bike_weather (
    fact_id          BIGINT,
    station_key      BIGINT,
    station_id       STRING,
    weather_key      BIGINT,
    weather_id       BIGINT,
    reading_ts       TIMESTAMP,
    free_bikes       INT,
    available_slots  INT,
    total_slots      INT,
    utilisation_rate DOUBLE,
    has_ebikes       BOOLEAN,
    batch_id         STRING,
    ingested_at      TIMESTAMP
)
""")

print("gold tables ready")