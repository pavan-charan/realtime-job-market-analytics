# Phase 4: Spark SQL ETL (Bronze → Silver → Gold Medallion Pipeline)

## 1. Objective
The objective of Phase 4 is to build the batch Spark SQL ETL pipeline implementing the **Medallion Lakehouse Architecture**:
1. **Bronze → Silver**: Cleanse raw Parquet records, handle missing/corrupt values, normalize city and country names, classify titles into standardized tech role categories and seniority levels, annualize salary ranges, and eliminate duplicate job postings.
2. **Silver → Gold**: Transform cleansed Silver data into a Kimball Star Schema (Fact and Dimension tables) and pre-aggregated analytical tables for instant Hive and Grafana dashboard querying.

---

## 2. Medallion ETL Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                 HDFS Bronze Layer (Parquet)                 │
│              hdfs://localhost:9000/data/job_market/bronze   │
└─────────────────────────────────────────────────────────────┘
                               │
                               │ Spark SQL Cleaning & Feature Extraction
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                 HDFS Silver Layer (Parquet)                 │
│              hdfs://localhost:9000/data/job_market/silver   │
│                                                             │
│  • Deduplicated by job_id                                   │
│  • Standardized Job Categories & Seniority Levels           │
│  • Normalized Locations & Remote Flags                      │
│  • Annualized USD Salaries (hourly/monthly normalized)      │
│  • Exploded & Cleaned Skill Arrays                          │
└─────────────────────────────────────────────────────────────┘
                               │
                               │ Star Schema Modeling & Aggregations
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                  HDFS Gold Layer (Parquet)                  │
│              hdfs://localhost:9000/data/job_market/gold/    │
│                                                             │
│  Star Schema Dimensions:                                    │
│  ├── dim_company          ├── dim_date                      │
│  ├── dim_job_role         ├── dim_skill                     │
│  └── dim_location         └── bridge_job_skills             │
│                                                             │
│  Fact Table:                                                │
│  └── fact_job_postings                                      │
│                                                             │
│  Analytical Aggregates:                                     │
│  ├── gold_top_skills_demand                                 │
│  ├── gold_salary_by_role_city                               │
│  ├── gold_remote_trends                                     │
│  └── gold_company_hiring                                    │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. Key Transformations & SQL Logic

### A. Role & Experience Level Derivation
Regex-based natural language categorization of job titles into standardized analytical roles:
- **Role Categories**: `Data Engineering`, `Data Science & AI`, `Data Analytics & BI`, `DevOps & Cloud`, `Software Engineering`, `Frontend & Web`, `Product Management`, `Cybersecurity`, `Other Tech Roles`.
- **Seniority Levels**: `Entry / Junior`, `Mid-Level`, `Senior`, `Lead / Principal`.

### B. Salary Imputation & Annualization
Calculates standardized annualized salaries across mixed pay frequencies (`hour`, `month`, `week`, `year`):
```sql
CASE
    WHEN LOWER(salary_period) = 'hour' THEN ((salary_min + salary_max) / 2.0) * 2080
    WHEN LOWER(salary_period) = 'month' THEN ((salary_min + salary_max) / 2.0) * 12
    WHEN LOWER(salary_period) = 'week' THEN ((salary_min + salary_max) / 2.0) * 52
    ELSE ((salary_min + salary_max) / 2.0)
END AS annual_salary_usd
```

### C. Dimensional Modeling (Star Schema)
Generates surrogate keys (`company_key`, `role_key`, `location_key`, `date_key`, `skill_key`) and establishes clean relational links from `fact_job_postings` to dimensions.

---

## 4. Commands to Execute

### Option A: Run End-to-End ETL (Bronze → Silver → Gold)
```bash
python spark/etl.py --mode all
```

### Option B: Run Specific Stage
```bash
# Execute Bronze to Silver only
python spark/etl.py --mode bronze-to-silver

# Execute Silver to Gold only
python spark/etl.py --mode silver-to-gold
```

### Option C: Submit to Spark Cluster via spark-submit
```bash
spark-submit \
  --master spark://localhost:7077 \
  --driver-memory 4g \
  --executor-memory 4g \
  spark/etl.py --mode all
```

### Step 2: Verify Transformed Data in HDFS
```bash
# Verify Silver layer Parquet
docker exec -it namenode hdfs dfs -ls /data/job_market/silver

# Verify Gold Star Schema tables
docker exec -it namenode hdfs dfs -ls /data/job_market/gold
```

---

## 5. Expected Output

### Spark SQL ETL Console Execution:
```text
2026-09-09 21:25:00 [INFO] [SparkSQLETL] Initializing Spark SQL Session (Master: local[*])...
2026-09-09 21:25:02 [INFO] [SparkSQLETL] Spark Session established. Version: 3.5.0
2026-09-09 21:25:02 [INFO] [SparkSQLETL] ==================================================
2026-09-09 21:25:02 [INFO] [SparkSQLETL] Starting Bronze -> Silver Transformation
2026-09-09 21:25:02 [INFO] [SparkSQLETL] Reading Bronze data from: hdfs://localhost:9000/data/job_market/bronze
2026-09-09 21:25:05 [INFO] [SparkSQLETL] Loaded 394300 records from Bronze.
2026-09-09 21:25:10 [INFO] [SparkSQLETL] Writing Silver layer to Parquet at: hdfs://localhost:9000/data/job_market/silver
2026-09-09 21:25:15 [INFO] [SparkSQLETL] Successfully wrote 394300 cleansed records to Silver layer.
2026-09-09 21:25:15 [INFO] [SparkSQLETL] ==================================================
2026-09-09 21:25:15 [INFO] [SparkSQLETL] Starting Silver -> Gold Transformation
2026-09-09 21:25:16 [INFO] [SparkSQLETL] Generating Star Schema Dimension Tables...
2026-09-09 21:25:18 [INFO] [SparkSQLETL] Generating Star Schema Fact Table (fact_job_postings)...
2026-09-09 21:25:20 [INFO] [SparkSQLETL] Generating Bridge Table (bridge_job_skills)...
2026-09-09 21:25:22 [INFO] [SparkSQLETL] Generating Gold Analytical Aggregate Tables...
2026-09-09 21:25:25 [INFO] [SparkSQLETL] ==================================================
2026-09-09 21:25:25 [INFO] [SparkSQLETL] Successfully generated all Gold Star Schema & Analytics tables in HDFS!
2026-09-09 21:25:25 [INFO] [SparkSQLETL] Fact Table Count: 394300 rows
2026-09-09 21:25:25 [INFO] [SparkSQLETL] ==================================================
```

---

## 6. Screenshots Placeholder

| Scenario | Screenshot Reference | Description |
| :--- | :--- | :--- |
| **Spark SQL ETL Execution** | `[Screenshot: spark_sql_etl_execution.png]` | Terminal showing complete Bronze → Silver → Gold ETL progress |
| **HDFS Gold Storage Structure** | `[Screenshot: hdfs_gold_tables_listing.png]` | HDFS listing of Fact, Dimension, and Aggregate Parquet folders |
| **Spark SQL Query Plan** | `[Screenshot: spark_ui_sql_plan.png]` | Spark UI SQL Tab showing physical execution plan and adaptive partition coalescing |

---

## 7. Common Errors & Fixes

| Issue / Error | Root Cause | Solution / Fix |
| :--- | :--- | :--- |
| `AnalysisException: Path does not exist: hdfs://.../bronze` | Bronze layer has not been populated by streaming query | If streaming has not yet run, the ETL script automatically falls back to `dataset/tech_job_postings.csv`, or run `python kafka/producer.py` and `python spark/streaming.py` first. |
| `OutOfMemoryError: Java heap space` | Large shuffle partitions or insufficient driver memory during full dataset join | Run with `--driver-memory 4g --executor-memory 4g` or increase heap in `config/config.yaml`. |
| `ParquetDecodingException` | Corrupted Parquet file from abrupt stream shutdown | Clean HDFS checkpoint and rerun Bronze stream or overwrite Silver/Gold directories. |
