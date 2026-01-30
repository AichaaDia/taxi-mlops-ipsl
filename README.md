# Complete MLOps Pipeline for Taxi Fare Prediction

## 📚 Educational Project Overview

This project demonstrates a **complete end-to-end MLOps pipeline** using Databricks Delta Live Tables (DLT) for NYC Yellow Taxi data. It showcases modern data engineering and machine learning practices including data ingestion, feature engineering, model training, and automated deployment with CI/CD.

**Target Audience:** Data Engineering and ML Engineering students  
**Technologies:** Databricks, Delta Live Tables, PySpark, GitHub Actions, Unity Catalog  
**Dataset:** NYC Yellow Taxi Trip Records

---

## 🎯 Learning Objectives

By studying this project, students will learn:

1. **Medallion Architecture** - Bronze, Silver, Gold data layers
2. **Feature Engineering** - Creating ML-ready features from raw data
3. **MLOps Best Practices** - Model training, versioning, and deployment
4. **Data Quality** - Implementing expectations and validations
5. **CI/CD for Data Pipelines** - Automated testing and deployment
6. **Incremental Processing** - Efficient data processing patterns

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                             │
│              /Volumes/taxi_mlops_prod/taxi_analytics/            │
│                        yellowdata (Parquet)                      │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER (Raw)                          │
│                   bronze_taxi_trips                              │
│  • Auto Loader ingestion                                         │
│  • Schema inference                                              │
│  • Streaming table                                               │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   SILVER LAYER (Cleaned)                         │
│                  silver_taxi_features                            │
│  • Data quality expectations                                     │
│  • Feature engineering:                                          │
│    - trip_duration_minutes                                       │
│    - speed_mph                                                   │
│    - time_of_day, day_of_week                                    │
│    - is_airport_pickup/dropoff                                   │
│  • Invalid record filtering                                      │
└────────────────────────────┬────────────────────────────────────┘
                             │
                ┌────────────┴────────────┐
                ▼                         ▼
┌──────────────────────────┐  ┌──────────────────────────────────┐
│   GOLD LAYER (Aggregated)│  │      ML LAYER (Training)         │
│                          │  │                                  │
│ • gold_hourly_location_  │  │ • ml_training_data               │
│   metrics                │  │   - Train/test split (80/20)     │
│ • gold_daily_time_       │  │   - Feature selection            │
│   patterns               │  │                                  │
│ • gold_location_pair_    │  │ • ml_model_training              │
│   metrics                │  │   - Linear regression model      │
│                          │  │   - RMSE, MAE, R² metrics        │
│ Partitioned by date      │  │                                  │
│ Incremental refresh      │  │ • ml_model_registry              │
│                          │  │   - Model metadata               │
│                          │  │   - Version tracking             │
│                          │  │                                  │
│                          │  │ • ml_predictions                 │
│                          │  │   - Batch inference (1000 rows)  │
│                          │  │   - Prediction error analysis    │
└──────────────────────────┘  └──────────────────────────────────┘
```

---

## 📁 Project Structure

```
taxi-mlops-ipsl/
│
├── transformations/              # Pipeline transformation code
│   ├── bronze/                   # Raw data ingestion
│   │   └── bronze_taxi_trips.py
│   │
│   ├── silver/                   # Cleaned and enriched data
│   │   └── silver_taxi_features.py
│   │
│   ├── gold/                     # Aggregated analytics
│   │   └── gold_taxi_aggregates.py
│   │
│   └── ml/                       # Machine learning components
│       ├── ml_training_data.py
│       ├── ml_model_training.py
│       ├── ml_model_registry.py
│       └── ml_predictions.py
│
├── cicd/                         # CI/CD automation
│   ├── trigger_pipeline.py       # Pipeline trigger script
│   └── CICD_SETUP_GUIDE.py      # Setup documentation
│
├── .github/workflows/            # GitHub Actions workflows
│   ├── databricks-pipeline.yml   # Main CI/CD workflow
│   ├── ci.yml                    # Continuous integration
│   └── deploy.yml                # Deployment workflow
│
└── README.md                     # This file
```

---

## 🔄 Data Flow Explained

### 1. Bronze Layer: Raw Data Ingestion

**File:** `transformations/bronze/bronze_taxi_trips.py`

**Purpose:** Ingest raw taxi trip data from cloud storage

**Key Concepts:**
- **Auto Loader (cloudFiles)**: Automatically detects and processes new files
- **Schema Inference**: Automatically determines data types
- **Streaming Table**: Continuously processes new data

**Code Highlights:**
```python
@dp.table(comment="Raw taxi trip data ingested from cloud storage")
def bronze_taxi_trips():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/taxi_mlops_prod/taxi_analytics/yellowdata")
    )
```

**What Students Learn:**
- How to use Auto Loader for scalable data ingestion
- Streaming vs batch processing
- Schema evolution handling

---

### 2. Silver Layer: Feature Engineering

**File:** `transformations/silver/silver_taxi_features.py`

**Purpose:** Clean data and create ML-ready features

**Key Concepts:**
- **Data Quality Expectations**: Validate data before processing
- **Feature Engineering**: Create derived features from raw data
- **Data Filtering**: Remove invalid or outlier records

**Features Created:**
1. **trip_duration_minutes**: Time between pickup and dropoff
2. **speed_mph**: Average speed during trip
3. **pickup_hour**: Hour of day (0-23)
4. **pickup_day_of_week**: Day of week (1-7)
5. **time_of_day**: Categorical (morning/afternoon/evening/night)
6. **is_airport_pickup/dropoff**: Boolean flags for airport trips

**Data Quality Rules:**
```python
@dp.expect_all_or_drop({
    "valid_trip_distance": "trip_distance > 0 AND trip_distance < 100",
    "valid_fare": "fare_amount > 0 AND fare_amount < 500",
    "valid_passenger_count": "passenger_count > 0 AND passenger_count <= 6",
    "valid_timestamps": "tpep_pickup_datetime < tpep_dropoff_datetime"
})
```

**What Students Learn:**
- Feature engineering techniques
- Data quality validation
- Domain knowledge application (e.g., airport codes)

---

### 3. Gold Layer: Aggregated Analytics

**File:** `transformations/gold/gold_taxi_aggregates.py`

**Purpose:** Create aggregated metrics for analytics and ML features

**Three Materialized Views:**

#### 3.1 Hourly Location Metrics
- Trip counts by location and hour
- Average trip metrics (distance, duration, fare, speed)
- Airport trip percentages
- **Use Case**: Demand forecasting, surge pricing

#### 3.2 Daily Time Patterns
- Demand patterns by time of day
- Day of week trends
- Payment type distributions
- **Use Case**: Operational planning, driver allocation

#### 3.3 Location Pair Metrics
- Popular routes (pickup → dropoff)
- Route efficiency metrics
- Minimum/maximum durations per route
- **Use Case**: Route optimization, ETA prediction

**Key Concepts:**
- **Materialized Views**: Pre-computed aggregations
- **Incremental Refresh**: Only process changed data (serverless only)
- **Partitioning**: Organize data by date for efficient queries

**What Students Learn:**
- Aggregation patterns for analytics
- Partitioning strategies
- Incremental vs full refresh

---

### 4. ML Layer: Model Training & Inference

#### 4.1 Training Data Preparation

**File:** `transformations/ml/ml_training_data.py`

**Purpose:** Prepare features for model training

**Key Features:**
- 80/20 train/test split using hash-based partitioning
- Feature selection (trip, time, location features)
- Target variable: `total_amount` (fare prediction)

#### 4.2 Model Training

**File:** `transformations/ml/ml_model_training.py`

**Purpose:** Train fare prediction model

**Model Type:** Linear Regression (SQL-based)

**Model Formula:**
```
predicted_fare = base_fare (3.0)
                + (distance × 2.5)
                + (duration × 0.5)
                + time_of_day_adjustment
                + airport_surcharge
                + (passenger_count × 0.5)
```

**Evaluation Metrics:**
- **RMSE** (Root Mean Square Error): Average prediction error
- **MAE** (Mean Absolute Error): Average absolute error
- **Correlation**: Relationship strength between predicted and actual

**What Students Learn:**
- Feature encoding (one-hot encoding for categorical variables)
- Model evaluation metrics
- Train/test split methodology

#### 4.3 Model Registry

**File:** `transformations/ml/ml_model_registry.py`

**Purpose:** Track model metadata and versions

**Stored Information:**
- Model name and version
- Performance metrics
- Training timestamp
- Model status (active/archived)

**What Students Learn:**
- Model versioning
- Model governance
- Metadata tracking

#### 4.4 Batch Predictions

**File:** `transformations/ml/ml_predictions.py`

**Purpose:** Generate predictions on new data

**Output:**
- 1000 sample predictions
- Actual vs predicted comparison
- Prediction error and error percentage

**What Students Learn:**
- Batch inference patterns
- Model deployment
- Prediction monitoring

---

## 🚀 CI/CD Pipeline

### GitHub Actions Workflow

**File:** `.github/workflows/databricks-pipeline.yml`

**Triggers:**
- Push to `main` or `develop` branches
- Pull requests to `main`
- Manual workflow dispatch

**Jobs:**

1. **validate-pipeline**: Syntax validation
2. **trigger-pipeline**: Start Databricks pipeline update
3. **dry-run-on-pr**: Validate on pull requests

**What Students Learn:**
- CI/CD best practices
- Automated testing
- Infrastructure as Code

---

## 🛠️ Setup Instructions

### Prerequisites

1. **Databricks Workspace** with Unity Catalog enabled
2. **GitHub Account** for version control
3. **Data Source**: NYC Yellow Taxi data in `/Volumes/taxi_mlops_prod/taxi_analytics/yellowdata`

### Step 1: Clone Repository

```bash
git clone https://github.com/YOUR_USERNAME/taxi-mlops-ipsl.git
cd taxi-mlops-ipsl
```

### Step 2: Configure Databricks

1. Create catalog: `taxi_mlops_prod`
2. Create schema: `default`
3. Upload data to volume: `/Volumes/taxi_mlops_prod/taxi_analytics/yellowdata`

### Step 3: Create Pipeline

1. Go to Databricks → Workflows → Delta Live Tables
2. Click "Create Pipeline"
3. Configure:
   - **Name**: Complete-MLOps-Pipeline
   - **Source Code**: `/Repos/YOUR_USERNAME/taxi-mlops-ipsl/transformations/**`
   - **Catalog**: `taxi_mlops_prod`
   - **Schema**: `default`
   - **Compute**: Serverless (recommended)

### Step 4: Configure GitHub Secrets

Add these secrets to your GitHub repository:

- `DATABRICKS_HOST`: Your workspace URL
- `DATABRICKS_TOKEN`: Personal access token
- `PIPELINE_ID`: Your pipeline ID

### Step 5: Run Pipeline

**Manual:**
```bash
# In Databricks UI
Click "Start" on your pipeline
```

**Via GitHub Actions:**
```bash
git add .
git commit -m "Update pipeline"
git push origin main
```

---

## 📊 Key Datasets

| Dataset Name | Type | Description | Records |
|-------------|------|-------------|---------|
| `bronze_taxi_trips` | Streaming Table | Raw taxi trip data | ~Millions |
| `silver_taxi_features` | Streaming Table | Cleaned with ML features | ~Millions |
| `gold_hourly_location_metrics` | Materialized View | Hourly aggregations | ~Thousands |
| `gold_daily_time_patterns` | Materialized View | Daily patterns | ~Hundreds |
| `gold_location_pair_metrics` | Materialized View | Route metrics | ~Thousands |
| `ml_training_data` | Materialized View | ML training dataset | ~Millions |
| `ml_model_training` | Materialized View | Model metrics | 3 rows |
| `ml_model_registry` | Materialized View | Model metadata | 1 row |
| `ml_predictions` | Materialized View | Sample predictions | 1000 rows |

---

## 🎓 Learning Exercises

### Exercise 1: Add New Features
**Task:** Add a new feature to predict if a trip will have a tip > $5

**Steps:**
1. Modify `silver_taxi_features.py`
2. Add feature: `high_tip = tip_amount > 5`
3. Update model to use this feature

### Exercise 2: Create New Aggregation
**Task:** Create a gold table for hourly revenue by payment type

**Steps:**
1. Create new file: `gold_revenue_analysis.py`
2. Aggregate by hour and payment_type
3. Calculate total revenue and trip counts

### Exercise 3: Improve Model
**Task:** Add more features to improve prediction accuracy

**Ideas:**
- Weather data (if available)
- Holiday indicator
- Traffic patterns
- Historical averages for route

### Exercise 4: Add Data Quality Alerts
**Task:** Set up alerts when data quality expectations fail

**Steps:**
1. Add more expectations to silver layer
2. Configure pipeline notifications
3. Create monitoring dashboard

### Exercise 5: Implement A/B Testing
**Task:** Compare two different models

**Steps:**
1. Create `ml_model_training_v2.py` with different formula
2. Compare metrics between v1 and v2
3. Document which performs better

---

## 🔍 Monitoring & Observability

### Pipeline Metrics

Monitor these in Databricks UI:

1. **Data Quality Metrics**
   - Expectation pass/fail rates
   - Records dropped
   - Data freshness

2. **Performance Metrics**
   - Processing time per layer
   - Data volume processed
   - Compute costs

3. **Model Metrics**
   - RMSE, MAE, R²
   - Prediction error distribution
   - Model drift over time

### Accessing Metrics

```sql
-- View data quality metrics
SELECT * FROM event_log('Complete-MLOps-Pipeline')
WHERE event_type = 'flow_progress'

-- View model performance
SELECT * FROM taxi_mlops_prod.default.ml_model_training

-- View prediction errors
SELECT 
  AVG(absolute_error) as avg_error,
  MAX(absolute_error) as max_error,
  PERCENTILE(absolute_error, 0.95) as p95_error
FROM taxi_mlops_prod.default.ml_predictions
```

---

## 🐛 Troubleshooting

### Common Issues

**Issue 1: Pipeline fails at bronze layer**
- **Cause**: Data source not accessible
- **Solution**: Verify volume path and permissions

**Issue 2: High prediction errors**
- **Cause**: Model too simple or data quality issues
- **Solution**: Add more features or improve data cleaning

**Issue 3: Slow incremental refresh**
- **Cause**: Not using serverless or large data volumes
- **Solution**: Enable serverless compute and optimize partitioning

**Issue 4: GitHub Actions fails**
- **Cause**: Invalid secrets or permissions
- **Solution**: Verify DATABRICKS_TOKEN and PIPELINE_ID

---

## 📚 Additional Resources

### Documentation
- [Databricks Delta Live Tables](https://docs.databricks.com/delta-live-tables/index.html)
- [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)

### Tutorials
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [MLOps on Databricks](https://www.databricks.com/solutions/mlops)
- [Feature Engineering](https://www.databricks.com/blog/2022/10/20/feature-engineering-databricks.html)

### NYC Taxi Dataset
- [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)

---

## 👥 Contributing

Students are encouraged to:
1. Fork this repository
2. Create feature branches
3. Submit pull requests with improvements
4. Document your changes

---

## 📝 License

This project is for educational purposes.

---

## 🙋 Questions & Support

For questions about this project:
1. Review the documentation in `cicd/CICD_SETUP_GUIDE.py`
2. Check the troubleshooting section above
3. Examine the code comments in each transformation file
4. Reach out to your instructor

---

## 🎯 Project Outcomes

After completing this project, students will be able to:

✅ Design and implement medallion architecture  
✅ Build production-grade data pipelines  
✅ Apply feature engineering techniques  
✅ Train and deploy ML models  
✅ Implement data quality checks  
✅ Set up CI/CD for data pipelines  
✅ Monitor and troubleshoot pipelines  
✅ Work with Unity Catalog for data governance  

---

**Happy Learning! 🚀**
