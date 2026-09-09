# Spark Module: Streaming Ingestion, ETL & Machine Learning

This module contains the Apache Spark processing components for the Job Market Intelligence Lakehouse.

## Components

| File | Phase | Description |
| :--- | :--- | :--- |
| **`streaming.py`** | Phase 3 | Spark Structured Streaming pipeline: Consumes Kafka `job_postings`, validates JSON schema, removes duplicates with watermarking, extracts skill count, and writes raw Bronze Parquet data to HDFS. |
| **`etl.py`** | Phase 4 | Spark SQL ETL: Bronze → Silver (cleaning, normalization, null removal) & Silver → Gold (analytical aggregates, star schema dimensions & facts). |
| **`feature_engineering.py`** | Phase 6 | Feature pipelines (`StringIndexer`, `OneHotEncoderEstimator`, `VectorAssembler`). |
| **`train_model.py`** | Phase 6 | Spark MLlib model training (`RandomForestRegressor`), hyperparameter evaluation, model persistence. |
| **`predict.py`** | Phase 6 | Batch and real-time salary inference writing to Hive `prediction_results`. |

---

## Phase 3 Usage: Spark Structured Streaming

### 1. Run Bronze Streaming Ingestion with Live Console Preview
```bash
python spark/streaming.py --console
```

### 2. Submit to Spark Cluster
```bash
spark-submit \
  --master spark://localhost:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark/streaming.py
```

### 3. Check HDFS Bronze Output Files
```bash
docker exec -it namenode hdfs dfs -ls /data/job_market/bronze
```
