# Notebook: /Repos/prod/taxi-mlops/01_data_ingestion.py
# Type: SQL + Python (Delta Live Tables)

import dlt
from pyspark.sql.functions import *

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# BRONZE LAYER: Raw data
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dlt.table(
    description="Raw NYC yellow taxi data - unprocessed",
    comment="Source: NYC TLC"
)
def bronze_yellow_trips():
    return spark.read.parquet(
        "/mnt/data/taxi/yellow_tripdata_2024-*.parquet"
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SILVER LAYER: Cleaned & validated
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dlt.table(
    description="Cleaned taxi trips with quality checks"
)
@dlt.expect("valid_fare", "fare_amount > 0")
@dlt.expect("valid_distance", "trip_distance > 0.1")
@dlt.expect("valid_passengers", "passenger_count > 0 AND passenger_count <= 8")
@dlt.expect("valid_timestamp", "tpep_pickup_datetime < tpep_dropoff_datetime")
def silver_yellow_trips():
    return (
        dlt.read("bronze_yellow_trips")
        .filter(col("passenger_count") > 0)
        .filter(col("trip_distance") > 0.1)
        .filter(col("fare_amount") > 0)
        .filter(col("tpep_pickup_datetime") < col("tpep_dropoff_datetime"))
        .dropDuplicates(["tpep_pickup_datetime", "PULocationID"])
        .withColumn("trip_duration_minutes", 
                   (col("tpep_dropoff_datetime").cast("long") - 
                    col("tpep_pickup_datetime").cast("long")) / 60)
        .withColumn("speed_mph",
                   col("trip_distance") / (col("trip_duration_minutes") / 60 + 0.1))
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GOLD LAYER: Business-ready aggregates
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dlt.table(
    description="Hourly demand by pickup location"
)
def gold_hourly_demand():
    return (
        dlt.read("silver_yellow_trips")
        .groupBy(
            to_date(col("tpep_pickup_datetime")).alias("date"),
            hour(col("tpep_pickup_datetime")).alias("hour"),
            col("PULocationID").alias("pickup_zone")
        )
        .agg(
            count("*").alias("trip_count"),
            avg(col("fare_amount")).alias("avg_fare"),
            avg(col("trip_distance")).alias("avg_distance"),
            avg(col("passenger_count")).alias("avg_passengers")
        )
        .filter(col("trip_count") > 0)
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Setup in Databricks UI:
# 1. Workflows → Create Pipeline
# 2. Choose: Serverless + Unity Catalog
# 3. Target: prod_catalog.taxi_analytics
# 4. Add this notebook as source
# 5. Click "Create" → Auto-runs