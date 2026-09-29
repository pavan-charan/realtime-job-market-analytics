# 6. Scope & Boundary Definitions

### In-Scope Functionality
- Streaming ingestion via Apache Kafka with configurable velocity and loop replay.
- Bronze Parquet Lakehouse persistence on Hadoop HDFS with 10-minute watermarking.
- Silver cleansing, deduplication, currency standardization, and categorical parsing.
- Gold Star Schema with 1 Fact table (`fact_job_postings`), 6 Dimension tables, and 4 Aggregation tables.
- 20 complex HiveQL business intelligence queries.
- Distributed MLlib Random Forest model training, evaluation, and pipeline serialization.
- Interactive Streamlit predictor application and multi-panel Grafana monitoring dashboards.

### Out-of-Scope Functionality
- Scraping live web HTML pages directly inside the cluster (handled upstream by external collectors).
- Real-time automated credit card transactions or paid candidate matching.
