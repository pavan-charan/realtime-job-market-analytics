# 10. Feature & Module Documentation

### Module 1: Kafka Streaming Ingestion (`kafka/producer.py`)
- **Purpose:** Ingests raw job postings, serializes records to JSON, enforces gzip compression, and streams to Kafka topic `job_postings`.
- **Key Parameters:** `--rate <eps>` (events per second), `--loop` (continuous replay), `--limit <count>`.
- **Live Telemetry:** Persists streaming velocity and sent counts directly to PostgreSQL `streaming_metrics` every 3 seconds.

### Module 2: Spark Structured Streaming (`spark/streaming.py`)
- **Purpose:** Consumes Kafka records, validates JSON schema, applies 10-minute watermarks, deduplicates by `job_id`, and writes Bronze Parquet partitions to HDFS.

### Module 3: Medallion ETL & Star Schema Engine (`spark/etl.py`)
- **Bronze $ightarrow$ Silver:** Cleans string encodings, normalizes currencies to USD annual figures, categorizes titles into 7 roles, and extracts skill lists.
- **Silver $ightarrow$ Gold:** Generates Dimension tables (`dim_company`, `dim_job_role`, `dim_location`, `dim_skill`, `dim_date`, `bridge_job_skills`), Fact table (`fact_job_postings`), and 4 analytical aggregates.

### Module 4: Distributed Salary Predictor (`spark/train_model.py`)
- **Purpose:** Trains Spark MLlib Random Forest Regressor over indexed features (role, seniority, city, remote flag, skill counts, primary skill) with automated pipeline persistence.
