# Spark Module: Streaming Ingestion, ETL & Machine Learning

This module contains the Apache Spark processing components for the Job Market Intelligence Lakehouse.

## Components

| File | Phase | Description |
| :--- | :--- | :--- |
| **`streaming.py`** | Phase 3 | Spark Structured Streaming pipeline: Consumes Kafka `job_postings`, validates JSON schema, removes duplicates with watermarking, extracts skill count, and writes raw Bronze Parquet data to HDFS. |
| **`etl.py`** | Phase 4 | Spark SQL ETL: Bronze → Silver (cleaning, normalization, null removal, role classification) & Silver → Gold (Star Schema facts/dimensions and analytical aggregates). |
| **`feature_engineering.py`** | Phase 6 | Feature pipelines (`StringIndexer`, `OneHotEncoder`, `VectorAssembler`). |
| **`train_model.py`** | Phase 6 | Spark MLlib model training (`RandomForestRegressor`), hyperparameter evaluation, model persistence. |
| **`predict.py`** | Phase 6 | Batch and real-time salary inference writing to Hive `prediction_results`. |

---

## Phase 6 Usage: Spark MLlib Machine Learning Pipeline

### 1. Train Random Forest Salary Model
```bash
python spark/train_model.py --trees 50 --depth 10
```

### 2. Run Interactive Ad-Hoc Salary Prediction
```bash
python spark/predict.py \
  --role "Data Engineering" \
  --experience "Senior" \
  --city "San Francisco" \
  --remote \
  --skills "python,spark,sql,kafka"
```

### 3. Run Batch Predictions on All Open Jobs
```bash
python spark/predict.py --batch
```
