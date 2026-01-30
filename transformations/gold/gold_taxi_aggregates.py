from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Hourly aggregated taxi metrics by pickup location for ML model features",
    partition_cols=["pickup_date"]
)
def gold_hourly_location_metrics():
    """
    Gold layer: Hourly aggregations by location
    Provides features for demand forecasting and location-based ML models:
    - Trip counts and demand patterns
    - Average trip metrics (distance, duration, fare, speed)
    - Revenue metrics
    - Airport trip percentages
    """
    return (
        spark.read.table("silver_taxi_features")
        .groupBy(
            "pickup_date",
            "pickup_hour",
            "PULocationID"
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("trip_duration_minutes").alias("avg_trip_duration"),
            F.avg("fare_amount").alias("avg_fare_amount"),
            F.avg("total_amount").alias("avg_total_amount"),
            F.avg("speed_mph").alias("avg_speed_mph"),
            F.avg("passenger_count").alias("avg_passenger_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.sum(F.when(F.col("is_airport_pickup"), 1).otherwise(0)).alias("airport_pickup_count"),
            F.sum(F.when(F.col("is_airport_dropoff"), 1).otherwise(0)).alias("airport_dropoff_count")
        )
        .withColumn("airport_pickup_pct", F.col("airport_pickup_count") / F.col("trip_count") * 100)
        .withColumn("airport_dropoff_pct", F.col("airport_dropoff_count") / F.col("trip_count") * 100)
    )


@dp.materialized_view(
    comment="Daily aggregated taxi metrics by time of day for ML model features",
    partition_cols=["pickup_date"]
)
def gold_daily_time_patterns():
    """
    Gold layer: Daily time-of-day patterns
    Provides features for time-based ML models:
    - Demand patterns by time of day
    - Day of week trends
    - Payment type distributions
    - Trip characteristic patterns
    """
    return (
        spark.read.table("silver_taxi_features")
        .groupBy(
            "pickup_date",
            "pickup_day_of_week",
            "time_of_day"
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("trip_duration_minutes").alias("avg_trip_duration"),
            F.avg("fare_amount").alias("avg_fare_amount"),
            F.avg("tip_amount").alias("avg_tip_amount"),
            F.avg("speed_mph").alias("avg_speed_mph"),
            F.sum("total_amount").alias("total_revenue"),
            F.countDistinct("PULocationID").alias("unique_pickup_locations"),
            F.countDistinct("DOLocationID").alias("unique_dropoff_locations"),
            F.sum(F.when(F.col("payment_type") == 1, 1).otherwise(0)).alias("credit_card_count"),
            F.sum(F.when(F.col("payment_type") == 2, 1).otherwise(0)).alias("cash_count")
        )
        .withColumn("credit_card_pct", F.col("credit_card_count") / F.col("trip_count") * 100)
        .withColumn("cash_pct", F.col("cash_count") / F.col("trip_count") * 100)
    )


@dp.materialized_view(
    comment="Location pair metrics for route optimization and demand prediction",
    partition_cols=["pickup_date"]
)
def gold_location_pair_metrics():
    """
    Gold layer: Pickup-Dropoff location pair analysis
    Provides features for route optimization and demand prediction:
    - Popular routes
    - Average metrics per route
    - Route efficiency metrics
    """
    return (
        spark.read.table("silver_taxi_features")
        .groupBy(
            "pickup_date",
            "PULocationID",
            "DOLocationID"
        )
        .agg(
            F.count("*").alias("route_trip_count"),
            F.avg("trip_distance").alias("avg_route_distance"),
            F.avg("trip_duration_minutes").alias("avg_route_duration"),
            F.avg("fare_amount").alias("avg_route_fare"),
            F.avg("speed_mph").alias("avg_route_speed"),
            F.min("trip_duration_minutes").alias("min_route_duration"),
            F.max("trip_duration_minutes").alias("max_route_duration")
        )
        .filter("route_trip_count >= 5")  # Filter for statistically significant routes
    )
