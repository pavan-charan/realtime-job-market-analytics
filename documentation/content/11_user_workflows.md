# 11. User & End-to-End Workflows

### 11.1 Real-Time Streaming Ingestion Flow
```text
User initiates Producer -> Reads CSV / API Feed -> JSON Serialization -> Kafka Topic 'job_postings'
-> Spark Structured Streaming Micro-Batch -> Schema Validation -> 10-Min Watermark Dedup
-> Partitioned Parquet Write to HDFS (/data/job_market/bronze) -> Telemetry update in PostgreSQL
```

### 11.2 Warehouse Analytical Workflow
```text
Raw Bronze Lakehouse -> Spark SQL Medallion Transformation -> Cleansed Silver Parquet
-> Star Schema Generation (Fact + 6 Dims) -> Hive Table Registration -> HiveQL BI Query Execution (Beeline)
-> Results Output to Analysts & Dashboards
```

### 11.3 Interactive ML Salary Prediction Flow
```text
User selects Role, Seniority, City, Remote Status, and Skills in Streamlit UI
-> Streamlit invokes PySpark / Pre-trained MLlib PipelineModel
-> StringIndexer & OneHotEncoder transform categorical inputs -> VectorAssembler builds feature vector
-> Random Forest Regressor predicts annual USD salary + 95% confidence interval
-> UI dynamically renders prediction card and seniority progression curve
```
