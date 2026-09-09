# Phase 6: Machine Learning with Spark MLlib (Salary Prediction)

## 1. Objective
The objective of Phase 6 is to build and train a distributed **Machine Learning Pipeline in Spark MLlib** to accurately estimate tech job compensation (`annual_salary_usd`). The pipeline features automated multi-stage feature engineering, model training with `RandomForestRegressor`, rigorous evaluation ($R^2$, $RMSE$, $MAE$), model persistence in HDFS, and an inference engine that outputs predictions to the Apache Hive `prediction_results` table for Grafana integration.

---

## 2. Machine Learning Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                 HDFS Silver Layer (Parquet)                 │
│              hdfs://localhost:9000/data/job_market/silver   │
└─────────────────────────────────────────────────────────────┘
                               │
                               │ Feature Extraction & Preprocessing
                               ▼
┌─────────────────────────────────────────────────────────────┐
│             Spark MLlib Feature Pipeline Stages             │
│                (spark/feature_engineering.py)               │
│                                                             │
│  1. StringIndexer (Categoricals -> Numerical Indices)       │
│     • role_category, experience_level, city, etc.           │
│  2. OneHotEncoder (Indices -> Sparse Vectors)               │
│  3. VectorAssembler (Categorical Vectors + Numerics)        │
│     • Features: is_remote (0/1), skills_count               │
└─────────────────────────────────────────────────────────────┘
                               │
                               │ 80/20 Train/Test Split
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                Random Forest Regression Model               │
│                    (spark/train_model.py)                   │
│                                                             │
│  • Hyperparameters: 50 Trees, Max Depth 10                  │
│  • Evaluation: R² Score, RMSE, MAE                          │
│  • Save Model: /data/job_market/models/salary_prediction_rf │
└─────────────────────────────────────────────────────────────┘
                               │
                               │ Batch & Interactive Inference
                               ▼
┌─────────────────────────────────────────────────────────────┐
│             Inference Engine (spark/predict.py)             │
│                                                             │
│  • Interactive CLI / Ad-Hoc Estimator                       │
│  • High-Throughput Batch Inference                          │
│  • Sinks to Hive Table: job_market_dw.prediction_results    │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. Feature Pipeline Stages

### Categorical & Numerical Features
- **Categorical Columns**: `role_category`, `experience_level`, `city_normalized`, `employment_type`, `primary_skill`.
- **Numerical Columns**: `is_remote_double`, `skills_count_double`.
- **Target Label**: `annual_salary_usd` (double).

### Pipeline Definition:
```python
# String Indexing
indexers = StringIndexer(inputCols=CATEGORICAL_COLS, outputCols=[...], handleInvalid="keep")

# One-Hot Encoding
encoders = OneHotEncoder(inputCols=[...], outputCols=[...], handleInvalid="keep")

# Vector Assembly
assembler = VectorAssembler(inputCols=encoded_cols + NUMERICAL_COLS, outputCol="features")

# Model
rf = RandomForestRegressor(featuresCol="features", labelCol="label", numTrees=50, maxDepth=10)
```

---

## 4. Commands to Execute

### Step 1: Train the Random Forest Model
```bash
python spark/train_model.py --trees 50 --depth 10
```

### Step 2: Test Single Interactive Ad-Hoc Prediction
```bash
python spark/predict.py \
  --role "Data Engineering" \
  --experience "Senior" \
  --city "San Francisco" \
  --remote \
  --skills "python,spark,sql,kafka,airflow"
```

### Step 3: Run Batch Prediction on Entire Dataset
```bash
python spark/predict.py --batch
```

### Step 4: Verify Predictions in Hive Data Warehouse
```bash
docker exec -it hive-server beeline -u 'jdbc:hive2://localhost:10000/job_market_dw' -n hive -p hivepassword -e "SELECT * FROM prediction_results LIMIT 10;"
```

---

## 5. Expected Output

### Model Training & Evaluation Output:
```text
2026-09-09 21:30:00 [INFO] [SparkMLTrain] ==================================================
2026-09-09 21:30:00 [INFO] [SparkMLTrain] Starting Spark MLlib Salary Model Training
2026-09-09 21:30:00 [INFO] [SparkMLTrain] Hyperparameters: Trees=50 | MaxDepth=10 | Split=0.80
2026-09-09 21:30:00 [INFO] [SparkMLTrain] ==================================================
2026-09-09 21:30:05 [INFO] [SparkMLTrain] Total records with valid salary labels: 184520
2026-09-09 21:30:06 [INFO] [SparkMLTrain] Dataset Split: 147616 Train records (80.0%) | 36904 Test records (20.0%)
2026-09-09 21:30:06 [INFO] [SparkMLTrain] Fitting Random Forest Regression Pipeline...
2026-09-09 21:30:25 [INFO] [SparkMLTrain] Model fitting completed in 19.42 seconds.
2026-09-09 21:30:28 [INFO] [SparkMLTrain] ==================================================
2026-09-09 21:30:28 [INFO] [SparkMLTrain] Model Evaluation Metrics on Test Set:
2026-09-09 21:30:28 [INFO] [SparkMLTrain]   • R² (Coefficient of Determination) : 0.8142
2026-09-09 21:30:28 [INFO] [SparkMLTrain]   • RMSE (Root Mean Squared Error)    : $24,812.50 USD
2026-09-09 21:30:28 [INFO] [SparkMLTrain]   • MAE  (Mean Absolute Error)        : $17,450.20 USD
2026-09-09 21:30:28 [INFO] [SparkMLTrain] ==================================================
2026-09-09 21:30:30 [INFO] [SparkMLTrain] Saving fitted pipeline model to: hdfs://localhost:9000/data/job_market/models/salary_prediction_rf ...
2026-09-09 21:30:32 [INFO] [SparkMLTrain] Model successfully saved.
```

### Interactive CLI Prediction Output:
```text
=======================================================
          JOB SALARY PREDICTION RESULT
=======================================================
  • Role Category      : Data Engineering
  • Seniority Level    : Senior
  • Location           : San Francisco (Remote: True)
  • Skills (5)         : python, spark, sql, kafka, airflow
-------------------------------------------------------
  >>> PREDICTED SALARY : $168,450.00 USD / Year
  >>> CONFIDENCE SCORE : 95.0%
=======================================================
```

---

## 6. Screenshots Placeholder

| Scenario | Screenshot Reference | Description |
| :--- | :--- | :--- |
| **Spark MLlib Training Logs** | `[Screenshot: spark_mllib_training_metrics.png]` | Terminal showing training progress and evaluation metrics ($R^2$, RMSE) |
| **CLI Interactive Prediction** | `[Screenshot: spark_cli_salary_prediction.png]` | Terminal showing formatted salary estimation card with confidence score |
| **Hive Prediction Results Table** | `[Screenshot: hive_prediction_results_query.png]` | Beeline query output from `prediction_results` table |

---

## 7. Common Errors & Fixes

| Issue / Error | Root Cause | Solution / Fix |
| :--- | :--- | :--- |
| `IllegalArgumentException: Field "annual_salary_usd" does not exist` | Silver data is missing normalized salary column | Run `python spark/etl.py --mode all` to regenerate cleaned Silver and Gold Parquet layers. |
| `Unseen label error during prediction` | New unseen categorical value passed to StringIndexer | The pipeline is built with `handleInvalid="keep"` to gracefully encode unseen levels as index 0 without crashing. |
| `OutOfMemoryError: Java heap space during RandomForest fitting` | Insufficient driver/executor memory for tree depth | Increase memory with `--driver-memory 4g --executor-memory 4g` or decrease tree depth: `--depth 8`. |
