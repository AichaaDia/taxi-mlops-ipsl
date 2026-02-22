from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Batch predictions with extended error diagnostics for analysis"
)
def ml_predictions():
    """
    ML Batch Inference + Extended Error Analysis
    
    - Applies trained model logic
    - Samples 1000 test rows
    - Computes prediction errors
    - Adds diagnostic indicators for error analysis
    """

    # Get 1000 sample rows from test data
    test_data = (
        spark.read.table("ml_training_data")
        .filter("is_training = false")
        .limit(1000)
    )
    
    predictions = (
        test_data
        
        # ===============================
        # Encodage des variables
        # ===============================
        
        .withColumn("time_morning", F.when(F.col("time_of_day") == "morning", 1.0).otherwise(0.0))
        .withColumn("time_afternoon", F.when(F.col("time_of_day") == "afternoon", 1.0).otherwise(0.0))
        .withColumn("time_evening", F.when(F.col("time_of_day") == "evening", 1.0).otherwise(0.0))
        .withColumn("time_night", F.when(F.col("time_of_day") == "night", 1.0).otherwise(0.0))
        
        .withColumn("payment_credit", F.when(F.col("payment_type") == 1, 1.0).otherwise(0.0))
        .withColumn("payment_cash", F.when(F.col("payment_type") == 2, 1.0).otherwise(0.0))
        
        .withColumn("airport_pickup_flag", F.col("is_airport_pickup").cast("double"))
        .withColumn("airport_dropoff_flag", F.col("is_airport_dropoff").cast("double"))
        
        # ===============================
        # FORMULE DU MODÈLE (ancienne version)
        # ===============================
        
    .withColumn(
        "predicted_total_amount",
        
        # Base fare
        F.lit(3.0) +
        
        # Distance
        (F.col("trip_distance") * 2.5) +
        
        # Duration
        (F.col("trip_duration_minutes") * 0.5) +
        
        # Time adjustments
        F.when(F.col("time_evening") == 1, 2.0).otherwise(0.0) +
        F.when(F.col("time_night") == 1, 3.0).otherwise(0.0) +
        
        # Airport surcharge
        F.when(F.col("airport_pickup_flag") == 1, 5.0).otherwise(0.0) +
        F.when(F.col("airport_dropoff_flag") == 1, 5.0).otherwise(0.0) +
        
        # Passenger effect
        (F.col("passenger_count") * 0.5) +
        
        # ===============================
        # 🔥 NOUVELLES AMÉLIORATIONS
        # ===============================
        
        # Interaction distance × peak hour
        (F.col("trip_distance") * F.col("peak_hour_flag") * 0.8) +
        
        # Long trip surcharge
        F.when(F.col("trip_category") == "long", 4.0).otherwise(0.0)
    )
        
        # ===============================
        # CALCUL DES ERREURS
        # ===============================
        
        .withColumn("prediction_error", 
                    F.col("predicted_total_amount") - F.col("target_total_amount"))
        
        .withColumn("absolute_error", 
                    F.abs(F.col("prediction_error")))
        
        .withColumn("error_percentage", 
                    (F.col("absolute_error") / F.col("target_total_amount")) * 100)
        
        # ===============================
        # INDICATEURS DIAGNOSTICS AJOUTÉS
        # ===============================
        
        # Indicateur erreur élevée (> 10 dollars)
        .withColumn("high_error_flag",
                    F.when(F.col("absolute_error") > 10, 1).otherwise(0))
        
        # Indicateur sous-prédiction
        .withColumn("under_prediction_flag",
                    F.when(F.col("prediction_error") < 0, 1).otherwise(0))
        
        # Indicateur sur-prédiction
        .withColumn("over_prediction_flag",
                    F.when(F.col("prediction_error") > 0, 1).otherwise(0))
    )
    
    return predictions.select(
        "trip_distance",
        "trip_duration_minutes",
        "trip_category",
        "peak_hour_flag",
        "multi_passenger_flag",
        "pickup_hour",
        "time_of_day",
        "is_airport_pickup",
        "is_airport_dropoff",
        "passenger_count",
        F.col("target_total_amount").alias("actual_total_amount"),
        "predicted_total_amount",
        "prediction_error",
        "absolute_error",
        "error_percentage",
        "high_error_flag",
        "under_prediction_flag",
        "over_prediction_flag"
    )

# Test CI/CD
