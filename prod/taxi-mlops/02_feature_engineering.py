import mlflow
from databricks.feature_store import FeatureStoreClient
from pyspark.sql.functions import *

fs = FeatureStoreClient()

# Load gold table
hourly_demand = spark.sql("""
    SELECT *
    FROM taxi_mlops_prod.taxi_analytics.gold_hourly_demand
    WHERE date >= '2024-01-01'
""")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Create demand features
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

demand_features = (
    hourly_demand
    .groupBy("pickup_zone")
    .agg(
        avg(col("trip_count")).alias("demand_mean"),
        stddev(col("trip_count")).alias("demand_std"),
        
        # Rolling stats (last 7 days)
        max(when(col("date") >= date_sub(current_date(), 7), col("trip_count")))
            .alias("demand_last_7d_max"),
        avg(when(col("date") >= date_sub(current_date(), 7), col("trip_count")))
            .alias("demand_last_7d_avg"),
    )
    .withColumn("created_at", current_timestamp())
)

# Register to Feature Store
fs.create_table(
    name="taxi_mlops_prod.taxi_ml.demand_features",
    primary_keys=["pickup_zone"],
    df=demand_features,
    description="Demand statistics by zone"
)

mlflow.log_text("Features registered to Feature Store", "feature_log.txt")
print("✅ Feature table created")