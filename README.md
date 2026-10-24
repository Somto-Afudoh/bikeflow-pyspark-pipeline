# bikeflow-pyspark-pipeline

End-to-end bike-sharing data pipeline that ingests live Citi Bike station data and NYC weather forecasts from public APIs, processes them at scale with PySpark, and models them into analytics-ready fact and dimension tables in Unity Catalog.

## Project Tasks Covered

1. **Warehouse setup** — create the Unity Catalog catalog, bronze/silver/gold schemas, a raw-data volume, and the gold star-schema tables (`setup_warehouse.py`).
2. **API ingestion** — pull Citi Bike station snapshots and hourly NYC weather forecasts, persist the raw JSON to a UC volume, and register them as bronze tables (`ingestion.py`).
3. **Data transformation** — flatten, type, clean, and enrich station and weather data into silver tables (`Transform` notebook).
4. **Data quality checks** — row counts, distinct stations, null/negative value checks on silver before promotion.
5. **Dimensional modeling** — build SCD-style `dim_station`, append-only `dim_weather`, and `fact_bike_weather` in the gold layer (`Load_to_gold.py`).
6. **Verification** — post-load row counts, station version history, and forecast capture counts.

## Technology Stack

* **Python 3** — orchestration glue, API calls (`requests`), JSON handling
* **PySpark (Spark SQL)** — scalable transformations, `explode`, `arrays_zip`, `MERGE INTO`, hash keys (`xxhash64`)
* **Databricks** — serverless compute, notebooks, and files
* **Unity Catalog** — data storage and governance (catalog `optiridecatalog`, schemas `bronze` / `silver` / `gold`, volume `bronze.raw`)
* **Public APIs**
  * CityBik.es — Citi Bike NYC live station network (`/v2/networks/citi-bike-nyc`)
  * Open-Meteo — hourly NYC weather forecast (temperature, precipitation, wind, cloud cover, humidity)

## Inputs and Outputs

### Inputs
* **Citi Bike network snapshot** — ~2,400 stations per batch: name, GPS coordinates, free bikes, total slots, ebike availability, last reading timestamp
* **Hourly weather forecast** — 168 hours (7 days) per batch: temperature (°C), precipitation (mm), wind speed (km/h), cloud cover (%), relative humidity (%)
* **Batch identifier** — every run gets a UTC batch ID (`YYYYMMDD_HHMMSS`) stamped onto every row, tying each piece of data back to the exact run that captured it

### Outputs

| Layer | Table | Grain | Description |
| --- | --- | --- | --- |
| Bronze | `bronze.bike_raw` | 1 row per batch | Raw nested JSON of the full station network |
| Bronze | `bronze.weather_raw` | 1 row per batch | Raw nested JSON of the forecast |
| Silver | `silver.station_status` | 1 row per station per batch | Clean, typed station readings with derived metrics |
| Silver | `silver.weather_hourly` | 1 row per hour per batch | Clean, typed hourly weather |
| Gold | `gold.dim_station` | 1 row per station **version** | Slowly-changing station dimension (SCD Type 2) |
| Gold | `gold.dim_weather` | 1 row per captured forecast hour | Append-only weather dimension preserving every forecast revision |
| Gold | `gold.fact_bike_weather` | 1 row per station reading | Fact table joining each reading to its station version and the hour's weather |

Raw JSON files are also archived under `/Volumes/optiridecatalog/bronze/raw/{bike,weather}/` for replayability.

## Process, Step by Step

1. **Set up the warehouse** — run `setup_warehouse.py` once to create the catalog, schemas, volume, and empty gold tables.
2. **Ingest** — `ingestion.py` calls both APIs, writes the raw JSON to the volume, and appends typed-with-`batch_id` copies to the bronze tables.
3. **Transform stations** — the `Transform` notebook:
   * Selects the latest batch from bronze.
   * `explode`s the nested `network.stations` array into one row per station (≈2,437 rows).
   * Casts each field to a proper type (`station_id`, `station_name`, `latitude`, `longitude`, `reading_ts`, `free_bikes`, `total_slots`, `has_ebikes`).
   * Derives `available_slots` (`total_slots − free_bikes`) and `utilisation_rate` (`free_bikes / total_slots`, rounded to 3 decimals, null-safe).
   * Drops null IDs/timestamps and duplicates on (`station_id`, `reading_ts`), then appends to `silver.station_status`.
4. **Transform weather** — Open-Meteo returns parallel arrays, so `arrays_zip` stitches them into records and `explode` produces one row per hour (168 rows); fields are cast and typed into `silver.weather_hourly` (deduplicated on `weather_ts` within the batch only — later forecast revisions are deliberately kept).
5. **Quality gate** — before promotion, a check on silver confirms row counts, distinct stations, reading-window sanity, and zero negative-bike or missing-slot rows.
6. **Load gold** — `Load_to_gold.py`:
   * **`dim_station` (SCD Type 2)** — fingerprints each station (name, lat, long, slots); if the fingerprint differs from the current row, the old version is closed (`valid_to` set, `is_current = false`) and a new version inserted with a hash of (station, effective timestamp) as its key.
   * **`dim_weather` (append-only)** — hashes each (hour, batch) into `weather_key`; a `MERGE ... WHEN NOT MATCHED INSERT` keeps every forecast revision as immutable history.
   * **`fact_bike_weather`** — each station reading is truncated to its hour (`date_trunc('hour', reading_ts)`), left-joined to that hour's weather (as captured in this batch) and to the current station version; a `MERGE` insert-only keeps each `fact_id` unique, preserving history.
7. **Verify** — row counts across all gold tables, stations with multiple versions, and how many times each forecast hour has been captured.

## Key Transformations

* **Flattening** — `explode` turns one nested network row into one row per station; `arrays_zip` + `explode` convert parallel weather lists into hourly rows.
* **Typing** — explicit `CAST`s give every column a clean type; ISO timestamps (with the `Z` suffix trimmed) become proper timestamps.
* **Derived metrics** — `available_slots` and `utilisation_rate` computed per reading.
* **Hash surrogate keys** — `xxhash64` generates stable `station_key`, `weather_id`/`weather_key`, and `fact_id`.
* **Change detection** — a concat-fingerprint join decides which station rows need a new version in gold.
* **Idempotent loads** — insert-only `MERGE` statements make the pipeline safe to re-run without duplicating or rewriting history.

## Results

* A single batch ingests ~2,400 station readings and 168 hours of forecast, producing 2,437 silver station rows and 168 silver weather rows.
* Quality gates confirmed zero negative bike counts and zero missing slot values in the processed batch.
* `fact_bike_weather` provides analysis-ready station utilization joined to the weather conditions at the same hour — enabling questions like *"how does rain affect bike availability?"*
* The SCD Type 2 station dimension preserves the full history of station changes (renames, relocations, capacity changes), and `dim_weather` preserves every forecast revision, so forecast accuracy can itself be analyzed.

## Recommendations

* **Schedule the pipeline** — run ingestion on a cadence (e.g., hourly) via a Databricks job to build a real time-series of station readings rather than isolated snapshots.
* **Add alerts on the quality gate** — turn the silver checks into automated assertions or table expectations so a bad batch halts promotion automatically.
* **Forecast-accuracy analytics** — with multiple batches captured, compare early forecasts against later ones for the same hour to quantify Open-Meteo's revision behavior and pick the most reliable capture.
* **Table maintenance** — apply liquid clustering on `fact_bike_weather` (e.g., on `reading_ts`, `station_id`) and schedule `OPTIMIZE`/vacuum as the append-only tables grow.
* **Backfill from the volume** — the archived raw JSON in `bronze.raw` allows full reprocessing of any batch if a transformation rule changes.
* **BI layer** — point a dashboard at `gold.fact_bike_weather` joined with `gold.dim_station` / `gold.dim_weather` for utilization heatmaps and weather-correlation reporting.

