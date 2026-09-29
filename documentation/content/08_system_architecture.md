# 8. System Architecture (High & Component Level)

### 8.1 Architectural Diagram

```text
[ Data Source / CSV Feeds ]
           │
           ▼
[ Apache Kafka Producer ] (kafka/producer.py)
           │
           ▼ (Topic: 'job_postings' @ port 9092)
[ Apache Kafka Broker + Zookeeper ]
           │
           ▼
[ PySpark Structured Streaming ] (spark/streaming.py)
   ├── Schema Validation
   ├── 10-Min Ingestion Watermark
   └── Deduplication Key: job_id
           │
           ▼
[ Hadoop HDFS Bronze Layer ] (/data/job_market/bronze)
           │
           ▼
[ PySpark Medallion ETL ] (spark/etl.py)
   ├── Silver Layer: Standardized USD Salaries & Cleaned Roles
   └── Gold Layer: Star Schema Dimensions & Fact Tables
           │
           ▼
[ Apache Hive Data Warehouse ] (hive/schema.sql + PostgreSQL Metastore)
   ├── 12 Relational & Analytics Tables
   └── 20 Pre-defined BI Queries (hive/queries.sql)
           │
     ┌─────┴────────────────────────────┐
     ▼                                  ▼
[ Spark MLlib Model Pipeline ]    [ Presentation Layer ]
 - StringIndexer & OneHotEncoder   - Streamlit Predictor App (8501)
 - VectorAssembler                 - Grafana Telemetry (3001)
 - Random Forest Regressor
```

### 8.2 Component Responsibilities
- **Kafka (`kafka:9092`):** Distributed log decoupling ingestion from downstream consumption.
- **Spark Streaming (`spark-master:7077`):** Stateful micro-batch ingestion and raw parquet serialization.
- **Hadoop HDFS (`namenode:9870`, `datanode:9864`):** Distributed fault-tolerant storage for all Lakehouse layers.
- **Apache Hive (`hive-server:10000`, `hive-metastore:9083`):** SQL analytical warehouse over HDFS with PostgreSQL backend.
- **Spark MLlib:** Distributed feature transformation and regression modeling.
