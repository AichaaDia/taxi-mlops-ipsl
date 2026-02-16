# Complete MLOps Pipeline for Taxi Fare Prediction

## 🎓 Institut Polytechnique de Saint Louis (IPSL)

**Course:** Data Engineering, AI Engineering and MLOps  
**Instructor:** Mbaye Babacar Gueye, PhD  
**Institution:** Institut Polytechnique de Saint Louis  
**Academic Year:** 2025-2026

---

## 📚 Educational Project Overview

This project demonstrates a **complete end-to-end MLOps pipeline** using Databricks Delta Live Tables (DLT) for NYC Yellow Taxi data. It showcases modern data engineering and machine learning practices including data ingestion, feature engineering, model training, and automated deployment with CI/CD.

**Target Audience:** IPSL Students - Data Engineering and ML Engineering  
**Technologies:** Databricks, Delta Live Tables, PySpark, GitHub Actions, Unity Catalog  
**Dataset:** NYC Yellow Taxi Trip Records  
**Cloud Provider:** AWS (Amazon Web Services)
---

## 📚 Deadline: 2026-02-21 before 11:59PM

---

## 🎯 Learning Objectives

By studying this project, IPSL students will learn:

1. **Medallion Architecture** - Bronze, Silver, Gold data layers
2. **Feature Engineering** - Creating ML-ready features from raw data
3. **MLOps Best Practices** - Model training, versioning, and deployment
4. **Data Quality** - Implementing expectations and validations
5. **CI/CD for Data Pipelines** - Automated testing and deployment with GitHub Actions
6. **Incremental Processing** - Efficient data processing patterns
7. **Cloud Data Engineering** - Working with AWS and Databricks
8. **Unity Catalog** - Data governance and catalog management

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                             │
│              /Vol<VosPrenomNoms>/taxi_mlops_prod/taxi_analytics/            │
│                        yellowdata (Parquet)                      │
│                         [AWS S3 Storage]                         │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER (Raw)                          │
│                   bronze_taxi_trips                              │
│  • Auto Loader ingestion (cloudFiles)                           │
│  • Schema inference                                              │
│  • Streaming table                                               │
│  • Serverless compute                                            │
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
                             │
                             ▼
                  ┌──────────────────────┐
                  │   CI/CD AUTOMATION   │
                  │   GitHub Actions     │
                  │  • Validate code     │
                  │  • Trigger pipeline  │
                  │  • Monitor results   │
                  └──────────────────────┘
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

**Purpose:** Ingest raw taxi trip data from cloud storage (AWS S3 via Unity Catalog Volumes)

**Key Concepts:**
- **Auto Loader (cloudFiles)**: Automatically detects and processes new files
- **Schema Inference**: Automatically determines data types
- **Streaming Table**: Continuously processes new data
- **Serverless Compute**: No cluster management required

**Code Highlights:**
```python
@dp.table(comment="Raw taxi trip data ingested from cloud storage")
def bronze_taxi_trips():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Vol<VosPrenomNoms>/taxi_mlops_prod/taxi_analytics/yellowdata")
    )
```

**What IPSL Students Learn:**
- How to use Auto Loader for scalable data ingestion
- Streaming vs batch processing
- Schema evolution handling
- Working with Unity Catalog Volumes

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
6. **is_airport_pickup/dropoff**: Boolean flags for airport trips (JFK, LaGuardia, Newark)

**Data Quality Rules:**
```python
@dp.expect_all_or_drop({
    "valid_trip_distance": "trip_distance > 0 AND trip_distance < 100",
    "valid_fare": "fare_amount > 0 AND fare_amount < 500",
    "valid_passenger_count": "passenger_count > 0 AND passenger_count <= 6",
    "valid_timestamps": "tpep_pickup_datetime < tpep_dropoff_datetime"
})
```

**What IPSL Students Learn:**
- Feature engineering techniques for ML
- Data quality validation patterns
- Domain knowledge application (e.g., NYC airport codes)
- PySpark DataFrame transformations

---

### 3. Gold Layer: Aggregated Analytics

**File:** `transformations/gold/gold_taxi_aggregates.py`

**Purpose:** Create aggregated metrics for analytics and ML features

**Three Materialized Views:**

#### 3.1 Hourly Location Metrics
- Trip counts by location and hour
- Average trip metrics (distance, duration, fare, speed)
- Airport trip percentages
- **Use Case**: Demand forecasting, surge pricing algorithms

#### 3.2 Daily Time Patterns
- Demand patterns by time of day
- Day of week trends
- Payment type distributions
- **Use Case**: Operational planning, driver allocation optimization

#### 3.3 Location Pair Metrics
- Popular routes (pickup → dropoff)
- Route efficiency metrics
- Minimum/maximum durations per route
- **Use Case**: Route optimization, ETA prediction models

**Key Concepts:**
- **Materialized Views**: Pre-computed aggregations for performance
- **Incremental Refresh**: Only process changed data (serverless only)
- **Partitioning**: Organize data by date for efficient queries

**What IPSL Students Learn:**
- Aggregation patterns for analytics
- Partitioning strategies for big data
- Incremental vs full refresh trade-offs
- Performance optimization techniques

---

### 4. ML Layer: Model Training & Inference

#### 4.1 Training Data Preparation

**File:** `transformations/ml/ml_training_data.py`

**Purpose:** Prepare features for model training

**Key Features:**
- 80/20 train/test split using hash-based partitioning
- Feature selection (trip, time, location features)
- Target variable: `total_amount` (fare prediction)
- Data filtering for quality

#### 4.2 Model Training

**File:** `transformations/ml/ml_model_training.py`

**Purpose:** Train fare prediction model

**Model Type:** Linear Regression (SQL-based implementation)

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
- **RMSE** (Root Mean Square Error): Average prediction error magnitude
- **MAE** (Mean Absolute Error): Average absolute error
- **Correlation**: Relationship strength between predicted and actual values

**What IPSL Students Learn:**
- Feature encoding (one-hot encoding for categorical variables)
- Model evaluation metrics and interpretation
- Train/test split methodology
- SQL-based ML implementation (pipeline-compatible)

#### 4.3 Model Registry

**File:** `transformations/ml/ml_model_registry.py`

**Purpose:** Track model metadata and versions

**Stored Information:**
- Model name and version
- Performance metrics (RMSE, MAE, correlation)
- Training timestamp
- Model status (active/archived)
- Model description and lineage

**What IPSL Students Learn:**
- Model versioning best practices
- Model governance and compliance
- Metadata tracking for reproducibility
- MLOps lifecycle management

#### 4.4 Batch Predictions

**File:** `transformations/ml/ml_predictions.py`

**Purpose:** Generate predictions on new data

**Output:**
- 1000 sample predictions from test set
- Actual vs predicted comparison
- Prediction error and error percentage
- Feature values for analysis

**What IPSL Students Learn:**
- Batch inference patterns
- Model deployment strategies
- Prediction monitoring and analysis
- Error analysis techniques

---

## 🚀 CI/CD Pipeline with GitHub Actions

### Workflow Overview

**File:** `.github/workflows/databricks-pipeline.yml`

**Triggers:**
- Push to `main` or `develop` branches
- Pull requests to `main`
- Manual workflow dispatch (for testing)

**Jobs:**

1. **validate-pipeline**: 
   - Validates Python syntax
   - Checks code quality
   - Runs on every commit

2. **trigger-pipeline**: 
   - Triggers Databricks pipeline update
   - Waits for completion
   - Reports status

3. **dry-run-on-pr**: 
   - Validates changes on pull requests
   - Comments on PR with validation results
   - Prevents broken code from merging

**What IPSL Students Learn:**
- CI/CD best practices for data pipelines
- Automated testing and validation
- Infrastructure as Code (IaC)
- GitHub Actions workflow design
- DevOps for data engineering

---

## 🛠️ Setup Instructions for IPSL Students

### Prerequisites

1. **Databricks Workspace** (provided by instructor)
2. **GitHub Account** (free account)
3. **Access to Unity Catalog**: `taxi_mlops_prod`
4. **Data Source**: NYC Yellow Taxi data in Unity Catalog Volume

### Step 1: Clone Repository

```bash
git clone https://github.com/bentechno/taxi-mlops-ipsl.git
cd taxi-mlops-ipsl
```

### Step 2: Verify Databricks Access

1. Log into Databricks workspace (credentials provided by instructor)
2. Verify access to catalog: `taxi_mlops_prod`
3. Check data availability: `/Vol<VosPrenomNoms>/taxi_mlops_prod/taxi_analytics/yellowdata`

### Step 3: Create Your Pipeline

1. Go to Databricks → **Workflows** → **Delta Live Tables**
2. Click **"Create Pipeline"**
3. Configure:
   - **Name**: `Complete-MLOps-Pipeline-[YourName]`
   - **Source Code**: `/Repos/mbayebabacar.gueye@bennen.tech/taxi-mlops-ipsl/transformations/**`
   - **Catalog**: `taxi_mlops_prod_[YourName]`
   - **Schema**: `default` (or create your own)
   - **Compute**: **Serverless** (recommended)
   - **Channel**: Current
   - **Configuration**: Add `spark.databricks.delta.properties.defaults.feature.timestampNtz` = `supported`

### Step 4: Configure GitHub Secrets (Optional - for CI/CD)

If setting up CI/CD, add these secrets to your GitHub repository:

- `DATABRICKS_HOST`: Your workspace URL
- `DATABRICKS_TOKEN`: Personal access token
- `PIPELINE_ID`: Your pipeline ID (found in pipeline URL)

### Step 5: Run Pipeline

**Manual Execution:**
1. In Databricks UI, go to your pipeline
2. Click **"Start"**
3. Monitor progress in the UI

**Via GitHub Actions:**
```bash
git add .
git commit -m "Update pipeline configuration"
git push origin main
```

---

## 📊 Key Datasets

| Dataset Name | Type | Description | Approx. Records |
|-------------|------|-------------|-----------------|
| `bronze_taxi_trips` | Streaming Table | Raw taxi trip data | ~9.5 Million |
| `silver_taxi_features` | Streaming Table | Cleaned with ML features | ~8.5 Million |
| `gold_hourly_location_metrics` | Materialized View | Hourly aggregations by location | ~210,000 |
| `gold_daily_time_patterns` | Materialized View | Daily time-of-day patterns | ~369 |
| `gold_location_pair_metrics` | Materialized View | Route-level metrics | ~235,000 |
| `ml_training_data` | Materialized View | ML training dataset with split | ~8.5 Million |
| `ml_model_training` | Materialized View | Model performance metrics | 1 row |
| `ml_model_registry` | Materialized View | Model metadata and versions | 1 row |
| `ml_predictions` | Materialized View | Sample predictions with errors | 1,000 rows |

---

## 📋 Student Assignments & Required Work

### Overview
You should use the available worskpace in Databricks but create your own tokens. All IPSL students must complete the following work as part of this course:

### Part 1: Pipeline Understanding & Setup (Mandatory)

**Tasks:**
1. ✅ Clone the repository and understand the project structure
2. ✅ Set up Databricks access and verify Unity Catalog connectivity
3. ✅ Create your personal pipeline in Databricks
4. ✅ Run the complete pipeline successfully (all 4 layers)
5. ✅ Verify all tables are created and contain data
6. ✅ Document any issues encountered and their solutions

**Deliverables:**
- Successfully running pipeline in your Databricks workspace
- Screenshots of each layer (bronze, silver, gold, ML) showing table creation
- A brief written summary of the pipeline architecture in your own words

**Evaluation Criteria:**
- All tables created successfully
- Understanding demonstrated in written summary
- Proper documentation of setup process

---

### Part 2: Feature Engineering Project (Mandatory)

**Objective:** Enhance the current silver layer with additional ML features

**Required Tasks:**
1. ✅ Analyze the current features in `silver_taxi_features.py`
2. ✅ Create at least **3 new features** beyond the existing ones
3. ✅ Add corresponding data quality expectations for new features
4. ✅ Document each feature with:
   - Feature name and description
   - Calculation method
   - Business justification
   - Expected data quality rules

**Examples of Features You Can Add:**
- Trip category (short, medium, long distance)
- Peak hour indicator (rush hour vs. off-peak)
- Weather impact estimation
- Seasonal patterns
- Route popularity score
- Tip likelihood indicator
- Multi-passenger trip indicator

**Deliverables:**
- Modified `silver_taxi_features.py` with new features
- Feature documentation (comments in code + separate document)
- Updated pipeline with new features working correctly
- Data quality metrics for new features

**Evaluation Criteria:**
- Quality and relevance of features (not arbitrary additions)
- Proper implementation with data quality checks
- Clear documentation and justification
- Successful execution without errors

---

### Part 3: Model Improvement & Analysis (Mandatory)

**Objective:** Improve the ML model performance and create analysis

**Required Tasks:**
1. ✅ Review current model performance metrics (RMSE, MAE, R²)
2. ✅ Identify poor prediction cases and analyze root causes
3. ✅ Implement at least **2 model improvements**:
   - Option A: Add new features from Part 2
   - Option B: Modify model formula with interaction terms
   - Option C: Adjust model parameters or logic
   - Option D: Add domain-specific adjustments
4. ✅ Compare new vs. old model performance
5. ✅ Document findings and recommendations

**Deliverables:**
- Comparison report: Old model vs. New model performance
- Analysis of prediction errors (where does model fail?)
- Recommendations for further improvements
- Updated model code with improvements
- SQL queries showing performance comparison

**Evaluation Criteria:**
- Demonstrated improvement in at least one metric
- Thorough analysis of model behavior
- Clear documentation of changes made
- Professional presentation of results

---

### Part 4: CI/CD Implementation (Mandatory)

**Objective:** Set up automated pipeline with GitHub Actions

**Required Tasks:**
1. ✅ Fork the repository to your personal GitHub account
2. ✅ Configure GitHub Secrets (DATABRICKS_HOST, DATABRICKS_TOKEN, PIPELINE_ID)
3. ✅ Set up CI/CD workflow for your pipeline
4. ✅ Test automated pipeline trigger via GitHub push
5. ✅ Document the CI/CD process and workflow

**Deliverables:**
- Successful GitHub Actions workflow execution
- Screenshots of workflow logs
- Documentation of:
  - How to trigger pipeline via GitHub
  - What validations are performed
  - How to monitor pipeline execution
  - Troubleshooting steps

**Evaluation Criteria:**
- Working CI/CD pipeline
- Proper secret management
- Clear documentation
- Successful automated execution

---

### Part 5: Portfolio Project - Choose One (Optional - for bonus/higher grades)

Choose ONE advanced project to enhance your portfolio:

#### Option A: Advanced ML with A/B Testing
- Implement two different model training approaches
- Create A/B testing framework
- Perform statistical analysis of results
- Deliverable: A/B test report with recommendations

#### Option B: Data Quality Dashboard
- Create monitoring and alerting system
- Build Databricks dashboard for quality metrics
- Set up automated quality reports
- Deliverable: Dashboard + automated quality report system

#### Option C: Production Monitoring & Alerting
- Implement comprehensive pipeline monitoring
- Add alert system for failures
- Create performance metrics tracking
- Deliverable: Monitoring dashboard + alert configuration

#### Option D: Data Documentation & Lineage
- Document data lineage from source to predictions
- Create data dictionary for all tables
- Build automated metadata documentation
- Deliverable: Complete data documentation package

#### Option E: Cost Optimization Study
- Analyze pipeline costs (compute, storage, data transfer)
- Identify optimization opportunities
- Implement at least 2 cost-saving improvements
- Deliverable: Cost analysis report with savings quantification

---

### Final Deliverables Checklist

**Required for All Students:**

- [ ] **Part 1 Complete**: Running pipeline + documentation
- [ ] **Part 2 Complete**: At least 3 new features implemented
- [ ] **Part 3 Complete**: Model improvements + analysis report
- [ ] **Part 4 Complete**: CI/CD workflow configured and tested
- [ ] **GitHub Repository**: Fork created with all changes pushed
- [ ] **Final Report**: Comprehensive project summary (2-3 pages)
- [ ] **Code Quality**: Comments, documentation, clean code
- [ ] **Testing**: All changes tested and working

**Optional for Bonus:**
- [ ] **Part 5 Complete**: One advanced project implemented

### Submission Instructions

1. **Push All Changes** to your GitHub repository
2. **Email Submission** to mbayebabacar@gmail.com with:
   - GitHub repository URL
   - Links to key documents/reports
   - List of completed work
   - The name of your Unity Catalog catalog


### Grading Breakdown

| Component | Weight | Notes |
|-----------|--------|-------|
| Part 1: Setup & Understanding | 15% | Demonstrates foundational knowledge |
| Part 2: Feature Engineering | 25% | Quality and relevance of features |
| Part 3: Model Improvement | 25% | Performance gains and analysis |
| Part 4: CI/CD Implementation | 20% | Automation and documentation |
| Part 5: Portfolio Project (bonus) | +15% | Can exceed 100% for extra credit |
| Code Quality & Documentation | 15% | Clarity, comments, organization |


---

## 🎓 Learning Exercises for IPSL Students

### Exercise 1: Add New Features (Beginner)
**Objective:** Enhance feature engineering

**Task:** Add a new feature to predict if a trip will have a tip > $5

**Steps:**
1. Modify `silver_taxi_features.py`
2. Add feature: `high_tip = tip_amount > 5`
3. Update model to use this feature
4. Compare model performance

**Expected Learning:** Feature engineering, boolean logic, model improvement

---

### Exercise 2: Create New Aggregation (Intermediate)
**Objective:** Practice aggregation patterns

**Task:** Create a gold table for hourly revenue by payment type

**Steps:**
1. Create new file: `transformations/gold/gold_revenue_analysis.py`
2. Aggregate by hour and payment_type
3. Calculate total revenue, trip counts, and average fare
4. Add partitioning by date

**Expected Learning:** Aggregations, partitioning, materialized views

---

### Exercise 3: Improve Model Accuracy (Advanced)
**Objective:** Enhance ML model performance

**Task:** Add more features to improve prediction accuracy

**Ideas to implement:**
- Add interaction features (distance × time_of_day)
- Create location popularity scores
- Add historical averages for routes
- Implement polynomial features

**Expected Learning:** Advanced feature engineering, model optimization

---

### Exercise 4: Implement Data Quality Monitoring (Intermediate)
**Objective:** Set up comprehensive data quality checks

**Task:** Add monitoring for data quality expectations

**Steps:**
1. Add more expectations to silver layer
2. Create a monitoring dashboard
3. Set up alerts for quality failures
4. Document quality metrics

**Expected Learning:** Data quality, monitoring, alerting

---

### Exercise 5: A/B Testing Framework (Advanced)
**Objective:** Compare multiple models

**Task:** Implement A/B testing for two different models

**Steps:**
1. Create `ml_model_training_v2.py` with different formula
2. Run both models on same test set
3. Compare metrics (RMSE, MAE, R²)
4. Document which performs better and why
5. Create visualization of results

**Expected Learning:** A/B testing, model comparison, statistical analysis

---

## 🔍 Monitoring & Observability

### Pipeline Metrics to Monitor

1. **Data Quality Metrics**
   - Expectation pass/fail rates
   - Records dropped due to quality issues
   - Data freshness (time since last update)

2. **Performance Metrics**
   - Processing time per layer
   - Data volume processed
   - Compute costs (serverless)
   - Memory usage

3. **Model Metrics**
   - RMSE, MAE, R² over time
   - Prediction error distribution
   - Model drift detection

### Accessing Metrics via SQL

```sql
-- View data quality metrics from event log
SELECT * FROM event_log('Complete-MLOps-Pipeline')
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC;

-- View model performance
SELECT * FROM taxi_mlops_prod.default.ml_model_training;

-- Analyze prediction errors
SELECT 
  AVG(absolute_error) as avg_error,
  MAX(absolute_error) as max_error,
  MIN(absolute_error) as min_error,
  PERCENTILE(absolute_error, 0.50) as median_error,
  PERCENTILE(absolute_error, 0.95) as p95_error
FROM taxi_mlops_prod.default.ml_predictions;

-- Check data freshness
SELECT 
  MAX(pickup_date) as latest_date,
  COUNT(*) as total_records
FROM taxi_mlops_prod.default.silver_taxi_features;
```

---

## 🐛 Troubleshooting Guide

### Common Issues for IPSL Students

**Issue 1: Pipeline fails at bronze layer**
- **Symptom**: "FILE_NOT_FOUND" or "Path does not exist"
- **Cause**: Data source not accessible or incorrect path
- **Solution**: 
  - Verify volume path: `/Vol<VosPrenomNoms>/taxi_mlops_prod/taxi_analytics/yellowdata`
  - Check Unity Catalog permissions
  - Contact instructor if data is missing

**Issue 2: High prediction errors in ML model**
- **Symptom**: RMSE > 20, low correlation
- **Cause**: Model too simple or data quality issues
- **Solution**: 
  - Add more features (see Exercise 3)
  - Improve data cleaning in silver layer
  - Check for outliers in training data

**Issue 3: Slow incremental refresh**
- **Symptom**: Pipeline takes too long to update
- **Cause**: Not using serverless or large data volumes
- **Solution**: 
  - Enable serverless compute
  - Optimize partitioning strategy
  - Check for data skew

**Issue 4: GitHub Actions workflow fails**
- **Symptom**: 403 error or authentication failure
- **Cause**: Invalid secrets or insufficient permissions
- **Solution**: 
  - Verify `DATABRICKS_HOST` and `DATABRICKS_TOKEN` secrets
  - Check `PIPELINE_ID` is correct
  - Ensure workflow has proper permissions (see workflow file)

**Issue 5: Cannot access pipeline tables**
- **Symptom**: "Table not found" errors
- **Cause**: Pipeline hasn't run successfully or permissions issue
- **Solution**:
  - Run pipeline at least once
  - Check Unity Catalog permissions
  - Verify catalog and schema names

---

## 📚 Additional Resources for IPSL Students

### Official Documentation
- [Databricks Delta Live Tables](https://docs.databricks.com/delta-live-tables/index.html)
- [Auto Loader Documentation](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Unity Catalog Guide](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)

### Learning Resources
- [Medallion Architecture Explained](https://www.databricks.com/glossary/medallion-architecture)
- [MLOps on Databricks](https://www.databricks.com/solutions/mlops)
- [Feature Engineering Best Practices](https://www.databricks.com/blog/2022/10/20/feature-engineering-databricks.html)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)

### NYC Taxi Dataset
- [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
- [Dataset Schema Information](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

### Course Materials
- Course slides and lectures (provided by Dr. Gueye)
- Lab exercises and assignments

---

## 👥 Contributing & Collaboration

IPSL students are encouraged to:
1. **Fork this repository** for your own experiments
2. **Create feature branches** for new features
3. **Submit pull requests** with improvements
4. **Document your changes** thoroughly
5. **Share learnings** with classmates
6. **Ask questions** during office hours

### Collaboration Guidelines
- Use descriptive commit messages
- Comment your code thoroughly
- Follow PEP 8 style guide for Python
- Test changes before submitting PRs
- Help fellow students in discussions

---

## 📝 Assessment & Grading

This project may be used for course assessment. Students will be evaluated on:

1. **Technical Implementation** (40%)
   - Correct implementation of pipeline layers
   - Code quality and organization
   - Proper use of Databricks features

2. **Feature Engineering** (20%)
   - Quality of derived features
   - Domain knowledge application
   - Data quality implementation

3. **MLOps Practices** (20%)
   - Model training and evaluation
   - CI/CD implementation
   - Documentation quality

4. **Innovation & Improvement** (20%)
   - Completion of exercises
   - Additional features or improvements
   - Problem-solving approach

---

## 🎯 Project Outcomes

After completing this project, IPSL students will be able to:

✅ Design and implement medallion architecture for data pipelines  
✅ Build production-grade data pipelines using Databricks DLT  
✅ Apply feature engineering techniques for ML models  
✅ Train and deploy ML models in a pipeline  
✅ Implement comprehensive data quality checks  
✅ Set up CI/CD for automated data pipeline deployment  
✅ Monitor and troubleshoot production pipelines  
✅ Work with Unity Catalog for data governance  
✅ Use GitHub Actions for automation  
✅ Apply MLOps best practices in real-world scenarios  

---

## 📧 Contact & Support

**Instructor:** Dr. Mbaye Babacar Gueye, mbayebabacar@gmail.com, Tel WA: remis au responsable  
**Institution:** Institut Polytechnique de Saint Louis (IPSL)  
**Course:** Data Engineering, AI Engineering and MLOps

For questions about this project:
1. Review this documentation thoroughly
2. Check the troubleshooting section
3. Examine code comments in transformation files
4. Attend office hours
5. Post questions in course discussion forum
6. Email instructor for urgent issues

---

## 🏆 Acknowledgments

- **Institut Polytechnique de Saint Louis** for providing infrastructure
- **Databricks** for the platform and documentation
- **NYC Taxi & Limousine Commission** for the dataset
- **IPSL Students** for feedback and contributions

---

**Bonne chance! 🚀**

*This project is part of the Data Engineering, AI Engineering and MLOps course at Institut Polytechnique de Saint Louis, taught by Dr. Mbaye Babacar Gueye.*
