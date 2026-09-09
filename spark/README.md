# Spark Module: Streaming Ingestion, ETL & Machine Learning

This module contains the Apache Spark processing components for the Job Market Intelligence Lakehouse.

## Components

| File | Phase | Description |
| :--- | :--- | :--- |
| **`streaming.py`** | Phase 3 | Spark Structured Streaming pipeline: Consumes Kafka `job_postings`, validates JSON schema, removes duplicates with watermarking, extracts skill count, and writes raw Bronze Parquet data to HDFS. |
| **`etl.py`** | Phase 4 | Spark SQL ETL: Bronze → Silver (cleaning, normalization, null removal, role classification) & Silver → Gold (Star Schema facts/dimensions and analytical aggregates). |
| **`feature_engineering.py`** | Phase 6 | Feature pipelines (`StringIndexer`, `OneHotEncoderEstimator`, `VectorAssembler`). |
| **`train_model.py`** | Phase 6 | Spark MLlib model training (`RandomForestRegressor`), hyperparameter evaluation, model persistence. |
| **`predict.py`** | Phase 6 | Batch and real-time salary inference writing to Hive `prediction_results`. |

---

## Phase 4 Usage: Spark SQL ETL Pipeline

### 1. Run Complete Medallion Pipeline (Bronze → Silver → Gold)
```bash
python spark/etl.py --mode all
```

### 2. Run Only Bronze → Silver Transformation
```bash
python spark/etl.py --mode bronze-to-silver
```

### 3. Run Only Silver → Gold Transformation
```bash
python spark/etl.py --mode silver-to-gold
```

### 4. Submit to Spark Cluster
```bash
spark-submit \
  --master spark://localhost:7077 \
  --driver-memory 4g \
  --executor-memory 4g \
  spark/etl.py --mode all
```
