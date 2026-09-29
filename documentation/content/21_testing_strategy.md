# 21. Testing Strategy & Test Matrix

| Component | Test Type | Execution Command | Success Criteria |
| :--- | :--- | :--- | :--- |
| **Kafka Producer** | Unit & Integration | `python kafka/producer.py --limit 100` | 100 messages acknowledged with 0 errors. |
| **Spark Streaming** | Integration | `python spark/streaming.py --console` | Micro-batches ingested and persisted to HDFS. |
| **Medallion ETL** | Batch ETL | `python spark/etl.py --mode all` | Bronze -> Silver -> Gold tables populated. |
| **Hive Warehouse** | SQL Regression | `hive/queries.sql` via Beeline | All 20 queries execute with valid result sets. |
| **MLlib Model** | Model Evaluation | `python spark/train_model.py` | Model achieves R^2 >= 0.85 on test split. |
| **Streamlit UI** | End-to-End UI | `streamlit run streamlit_app.py` | UI renders and predicts live salaries. |
