# 28. Glossary & Domain Terminology

- **Bronze Layer:** Raw, unprocessed, immutable streaming data stored as Parquet on HDFS.
- **Silver Layer:** Cleansed, deduplicated, and currency-standardized dataset.
- **Gold Layer:** Dimensionally modeled Star Schema tables and business aggregates.
- **Watermarking:** Spark streaming technique to track event time and statefully drop late duplicate events.
- **Beeline:** Command-line interface for executing SQL queries against Apache HiveServer2.
- **VectorAssembler:** Spark MLlib transformer that combines multiple feature columns into a single vector.
