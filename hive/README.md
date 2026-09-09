# Hive Data Warehouse Module: Star Schema & Analytical Queries

This module contains the Apache Hive DDL schemas, external Parquet table mappings, and 20 business intelligence queries for the Job Market Intelligence Lakehouse.

## Components

- **`schema.sql`**: Full DDL script creating `job_market_dw` database, Star Schema external tables (`dim_company`, `dim_job_role`, `dim_location`, `dim_date`, `dim_skill`, `bridge_job_skills`, `fact_job_postings`), pre-aggregated Gold analytics tables, and the Spark MLlib `prediction_results` sink.
- **`warehouse_queries.sql`**: 20 comprehensive business intelligence SQL queries for market KPIs, skill demand, salary benchmarking, and remote work trends.

## Execution Commands

### 1. Initialize Hive Schema & Star Schema Tables
```bash
docker exec -i hive-server beeline -u 'jdbc:hive2://localhost:10000/default' -n hive -p hivepassword -f /hive/schema.sql
```
*Or copy and run inside hive-server container:*
```bash
docker cp hive/schema.sql hive-server:/tmp/schema.sql
docker exec -it hive-server beeline -u 'jdbc:hive2://localhost:10000/default' -n hive -p hivepassword -f /tmp/schema.sql
```

### 2. Execute Analytical Warehouse Queries
```bash
docker cp hive/warehouse_queries.sql hive-server:/tmp/warehouse_queries.sql
docker exec -it hive-server beeline -u 'jdbc:hive2://localhost:10000/job_market_dw' -n hive -p hivepassword -f /tmp/warehouse_queries.sql
```

### 3. Interactive Hive Beeline CLI
```bash
docker exec -it hive-server beeline -u 'jdbc:hive2://localhost:10000/job_market_dw' -n hive -p hivepassword
```
