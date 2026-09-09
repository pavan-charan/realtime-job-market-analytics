# Real-Time Big Data Pipeline for Job Market Intelligence

An end-to-end distributed Big Data engineering and machine learning platform analyzing real-time tech job postings and salary dynamics using **Apache Kafka**, **Apache Spark Structured Streaming**, **Apache Hadoop HDFS**, **Apache Hive**, **Spark MLlib**, and **Grafana**.

---

## 🏛️ Medallion Architecture

```
[Historical CSV Dataset (394k records)]
                   │
                   ▼
       [Kafka Producer (Python)]
                   │
                   ▼
     [Kafka Topic: job_postings]
                   │
                   ▼
   [Spark Structured Streaming]
                   │
                   ▼
  [HDFS Bronze Layer (Raw Ingest)]
                   │
                   ▼
  [Spark SQL ETL & Cleaning Engine]
                   │
                   ▼
 [HDFS Silver Layer (Cleaned & Typed)]
                   │
                   ▼
   [HDFS Gold Layer (Aggregations)]
                   │
                   ▼
   [Apache Hive Data Warehouse (Star Schema)]
                   │
         ┌─────────┴─────────┐
         ▼                   ▼
[Spark MLlib Pipeline]   [Grafana Dashboards]
 (Salary Prediction)       (1. Streaming Health)
         │                 (2. Market Intelligence)
         ▼                 (3. Salary Predictor UI)
[Hive: prediction_results]
```

---

## 📁 Repository Structure

```
job-market-intelligence/
├── config/
│   └── config.yaml               # Centralized pipeline configuration
├── dataset/
│   └── README.md                 # Dataset placeholder & instructions
├── docker/
│   └── docker-compose.yml        # Kafka, Spark, HDFS, Hive, Postgres, Grafana
├── docs/
│   └── phase1_setup.md           # Phase 1 setup & verification guide
├── grafana/
│   └── dashboard.json            # Monitoring & BI dashboards
├── hive/
│   ├── schema.sql                # Star schema DDL (Fact & Dimensions)
│   └── warehouse_queries.sql     # 20 Analytical Business Queries
├── kafka/
│   ├── producer.py               # Streaming replay producer
│   └── consumer.py               # Test streaming consumer
├── spark/
│   ├── streaming.py              # Kafka -> HDFS Bronze Structured Streaming
│   ├── etl.py                    # Bronze -> Silver -> Gold Spark SQL ETL
│   ├── feature_engineering.py    # ML Feature Prep & VectorAssembler
│   ├── train_model.py            # Spark MLlib Random Forest Training
│   └── predict.py                # Batch/Interactive Salary Inference
├── .env                          # Environment variables & ports
├── requirements.txt              # Python 3.11 dependencies
└── README.md
```

---

## 🚀 Development Phases

- [x] **Phase 1: Environment Setup** (Docker Compose, HDFS, Hive, Kafka, Spark, Grafana, Config)
- [ ] **Phase 2: Kafka Producer** (Real-Time CSV Replay, Configurable Rate, JSON Serialization)
- [ ] **Phase 3: Spark Structured Streaming** (Stream Ingestion, Deduplication, Skill Extraction, HDFS Bronze)
- [ ] **Phase 4: Spark SQL ETL** (Medallion Transformation, Normalization, Silver & Gold Aggregations)
- [ ] **Phase 5: Hive Data Warehouse** (Star Schema, Fact/Dim tables, 20 Analytical Queries)
- [ ] **Phase 6: Machine Learning** (Spark MLlib Pipeline, Random Forest Regression, Model Persistence)
- [ ] **Phase 7: Grafana Dashboards** (Streaming Health, Market Analytics, Prediction Interface)

---

## ⚙️ Quick Start (Phase 1)

See the comprehensive [Phase 1 Setup Guide](file:///c:/Users/sirap/OneDrive/Desktop/Sem-7/Big%20data/Project/job-market-intelligence/docs/phase1_setup.md) for full execution steps and verification.
