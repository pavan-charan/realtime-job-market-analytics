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
│   ├── README.md                 # Dataset documentation
│   └── tech_job_postings.csv     # 394,300+ Job postings dataset
├── docker/
│   └── docker-compose.yml        # Kafka, Spark, HDFS, Hive, Postgres, Grafana
├── docs/
│   ├── phase_2_guide.md          # Kafka Producer & Ingestion Guide
│   ├── phase_3_guide.md          # Spark Structured Streaming & Bronze Guide
│   ├── phase_4_guide.md          # Spark SQL Medallion ETL Guide
│   ├── phase_5_guide.md          # Hive Star Schema & 20 BI Queries Guide
│   ├── phase_6_guide.md          # Spark MLlib Machine Learning Guide
│   └── phase_7_guide.md          # Grafana Dashboards & Visualization Guide
├── grafana/
│   ├── dashboard.json            # 3-in-1 Monitoring, BI & ML Dashboard
│   ├── predict_app.py            # Streamlit Interactive ML Prediction Web App
│   └── provisioning/             # Auto-provisioned datasources & dashboards
├── hive/
│   ├── schema.sql                # Star schema DDL (Fact & Dimensions)
│   └── warehouse_queries.sql     # 20 Analytical Business Queries
├── kafka/
│   ├── producer.py               # Streaming replay producer (10 eps default)
│   └── consumer.py               # CLI diagnostic stream consumer
├── spark/
│   ├── streaming.py              # Kafka -> HDFS Bronze Structured Streaming
│   ├── etl.py                    # Bronze -> Silver -> Gold Spark SQL ETL
│   ├── feature_engineering.py    # ML Feature Prep & VectorAssembler
│   ├── train_model.py            # Spark MLlib Random Forest Training
│   └── predict.py                # Batch & Interactive Salary Inference
├── .env                          # Environment variables & ports
├── requirements.txt              # Python 3.11 dependencies
└── README.md
```

---

## 🚀 Development Phases (All Completed)

- [x] **Phase 1: Environment Setup** (Docker Compose, HDFS, Hive, Kafka, Spark, Grafana, Config)
- [x] **Phase 2: Kafka Producer** (Real-Time CSV Replay, Configurable Rate, JSON Serialization)
- [x] **Phase 3: Spark Structured Streaming** (Stream Ingestion, Deduplication, Skill Extraction, HDFS Bronze)
- [x] **Phase 4: Spark SQL ETL** (Medallion Transformation, Normalization, Silver & Gold Aggregations)
- [x] **Phase 5: Hive Data Warehouse** (Star Schema, Fact/Dim tables, 20 Analytical Queries)
- [x] **Phase 6: Machine Learning** (Spark MLlib Pipeline, Random Forest Regression, Model Persistence)
- [x] **Phase 7: Grafana Dashboards** (Streaming Health, Market Analytics, Prediction Interface)

---

## ⚙️ Quick Start End-to-End Execution

### 1. Launch Infrastructure
```bash
docker-compose -f docker/docker-compose.yml up -d
```

### 2. Start Real-Time Kafka Streaming
```bash
python kafka/producer.py --rate 10
```

### 3. Start Spark Structured Streaming (Bronze Ingestion)
```bash
python spark/streaming.py --console
```

### 4. Execute Medallion Spark SQL ETL (Bronze → Silver → Gold)
```bash
python spark/etl.py --mode all
```

### 5. Initialize Hive Star Schema & Run BI Queries
```bash
docker cp hive/schema.sql hive-server:/tmp/schema.sql
docker exec -it hive-server beeline -u 'jdbc:hive2://localhost:10000/default' -n hive -p hivepassword -f /tmp/schema.sql

docker cp hive/warehouse_queries.sql hive-server:/tmp/warehouse_queries.sql
docker exec -it hive-server beeline -u 'jdbc:hive2://localhost:10000/job_market_dw' -n hive -p hivepassword -f /tmp/warehouse_queries.sql
```

### 6. Train Spark MLlib Salary Prediction Model
```bash
python spark/train_model.py --trees 50 --depth 10
```

### 7. Run Ad-Hoc Salary Inference & Launch Web UI
```bash
# Test single interactive prediction
python spark/predict.py --role "Data Engineering" --experience "Senior" --city "San Francisco" --remote --skills "python,spark,sql,kafka"

# Run interactive web predictor app
streamlit run grafana/predict_app.py
```

### 8. View Grafana Dashboards
- Open `http://localhost:3000` (User: `admin` | Pass: `admin`)
- Navigate to **Dashboards → Job Market Intelligence**
