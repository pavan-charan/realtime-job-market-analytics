# Phase 5: Apache Hive Data Warehouse (Star Schema & BI Queries)

## 1. Objective
The objective of Phase 5 is to establish the enterprise **Data Warehouse layer in Apache Hive**. It creates the database `job_market_dw` and maps external Parquet tables to the HDFS Gold layer using Kimball Star Schema modeling. It also provides a suite of **20 deep analytical SQL queries** answering critical business intelligence and labor market questions.

---

## 2. Kimball Star Schema Architecture

```
                  ┌───────────────────────────────┐
                  │          dim_company          │
                  ├───────────────────────────────┤
                  │ PK  company_key (BIGINT)      │
                  │     company_name (STRING)     │
                  │     company_domain (STRING)   │
                  │     company_industry (STRING) │
                  │     total_jobs_posted (BIGINT)│
                  └──────────────┬────────────────┘
                                 │ 1:N
                                 ▼
┌──────────────────────────────┐ │ ┌──────────────────────────────┐
│         dim_job_role         │ │ │         dim_location         │
├──────────────────────────────┤ │ ├──────────────────────────────┤
│ PK  role_key (BIGINT)        │ │ │ PK  location_key (BIGINT)    │
│     role_category (STRING)   │ │ │     city (STRING)            │
│     experience_level (STRING)│ │ │     country (STRING)         │
└──────────────┬───────────────┘ │ │     is_remote (BOOLEAN)      │
               │ 1:N             │ └──────────────┬───────────────┘
               ▼                 ▼                ▼ 1:N
┌─────────────────────────────────────────────────────────────────┐
│                       fact_job_postings                         │
├─────────────────────────────────────────────────────────────────┤
│ PK  fact_key (BIGINT)                                           │
│     job_id (STRING) [Natural Key]                               │
│ FK  company_key (BIGINT)                                        │
│ FK  role_key (BIGINT)                                           │
│ FK  location_key (BIGINT)                                       │
│ FK  date_key (INT)                                              │
│     title (STRING)                                              │
│     annual_salary_usd (DOUBLE)                                  │
│     salary_min (DOUBLE)                                         │
│     salary_max (DOUBLE)                                         │
│     employment_type (STRING)                                    │
│     is_remote (BOOLEAN)                                         │
│     skills_count (INT)                                          │
│     url (STRING)                                                │
└────────────────────────────────┬────────────────────────────────┘
               ▲ 1:N             │ 1:N
               │                 ▼
┌──────────────┴───────────────┐ ┌────────────────────────────────┐
│           dim_date           │ │       bridge_job_skills        │
├──────────────────────────────┤ ├────────────────────────────────┤
│ PK  date_key (INT)           │ │ FK  job_id (STRING)            │
│     full_date (DATE)         │ │ FK  skill_key (BIGINT)         │
│     year (INT)               │ └──────────────┬─────────────────┘
│     month (INT)              │                │ N:1
│     quarter (INT)            │                ▼
│     day_name (STRING)        │ ┌────────────────────────────────┐
│     month_name (STRING)      │ │           dim_skill            │
└──────────────────────────────┘ ├────────────────────────────────┤
                                 │ PK  skill_key (BIGINT)         │
                                 │     skill_name (STRING)        │
                                 └────────────────────────────────┘
```

---

## 3. The 20 Business Intelligence SQL Queries

| # | Query Theme | Business Value |
| :--- | :--- | :--- |
| **Q1** | Overall Job Market KPI Card | Total jobs, average compensation, employer count, and remote share. |
| **Q2** | Top 15 Most In-Demand Skills | High-volume skills across all tech disciplines. |
| **Q3** | Highest Paying Tech Skills (>= 50 sample) | Identifies skills with maximum salary premium. |
| **Q4** | Salary Benchmark by Role Category | Comparative baseline for Data, AI, Cloud, and Software roles. |
| **Q5** | Seniority & Experience Progression | Salary increase trajectory across junior, mid, senior, and lead. |
| **Q6** | Top 10 Tech Hiring Hubs | Geographic analysis of job concentration and regional compensation. |
| **Q7** | Remote Work Adoption by Role | Determines which tech specializations have highest remote ratios. |
| **Q8** | Top 20 Active Hiring Companies | Industry sector, open headcounts, and remote flexibility. |
| **Q9** | Remote vs Onsite Salary Delta | Evaluates if remote jobs pay higher or lower than in-person roles. |
| **Q10** | Monthly Hiring Velocity Trend | Longitudinal timeline tracking job postings over time. |
| **Q11** | Core Skills for Data & AI Roles | Targeted skill requirements for Data Engineers and Data Scientists. |
| **Q12** | High-Paying Remote Opportunities | Roles offering > $150k USD full remote compensation. |
| **Q13** | Seniority Breakdown in Top 5 Employers | Examines organizational hierarchy and hiring seniority. |
| **Q14** | Highest Remote % by Individual Skill | Pinpoints niche skills associated with remote positions. |
| **Q15** | Quarter-over-Quarter (QoQ) Volume | Seasonal hiring trends by engineering discipline. |
| **Q16** | Multi-Skill Salary Premium | Quantifies salary increase based on skill count breadth (0 vs 5+). |
| **Q17** | Industry Sector Compensation | Compares FinTech, HealthTech, AI, and SaaS salaries. |
| **Q18** | Salary Variance & Spread by Role | Measures salary dispersion and volatility per discipline. |
| **Q19** | Employment Type Comparison | Full-time vs Contract vs Part-time volumes and earnings. |
| **Q20** | Leadership & Principal Hubs | Distribution of Staff/Principal/Lead roles across top cities. |

---

## 4. Commands to Execute

### Step 1: Ensure Hive Metastore & HiveServer2 are Healthy
```bash
docker-compose -f docker/docker-compose.yml ps
```

### Step 2: Apply Hive DDL Schema
```bash
# Copy schema file to Hive container and execute via Beeline
docker cp hive/schema.sql hive-server:/tmp/schema.sql
docker exec -it hive-server beeline -u 'jdbc:hive2://localhost:10000/default' -n hive -p hivepassword -f /tmp/schema.sql
```

### Step 3: Run the 20 BI Queries
```bash
docker cp hive/warehouse_queries.sql hive-server:/tmp/warehouse_queries.sql
docker exec -it hive-server beeline -u 'jdbc:hive2://localhost:10000/job_market_dw' -n hive -p hivepassword -f /tmp/warehouse_queries.sql
```

### Step 4: Interactive Querying via Beeline CLI
```bash
docker exec -it hive-server beeline -u 'jdbc:hive2://localhost:10000/job_market_dw' -n hive -p hivepassword
```
```sql
SHOW TABLES;
SELECT * FROM fact_job_postings LIMIT 5;
```

---

## 5. Expected Output

### Beeline Execution Output:
```text
Connected to: Apache Hive (version 3.1.3)
Driver: Hive JDBC Driver

+--------------------------+
|         tab_name         |
+--------------------------+
| bridge_job_skills        |
| dim_company              |
| dim_date                 |
| dim_job_role             |
| dim_location             |
| dim_skill                |
| fact_job_postings        |
| gold_company_hiring      |
| gold_remote_trends       |
| gold_salary_by_role_city |
| gold_top_skills          |
| prediction_results       |
+--------------------------+

Running Query 1: Overall Job Market Summary KPI Card
+---------------------+-----------------------+--------------------------+--------------------+
| total_job_postings  | avg_annual_salary_usd | total_hiring_companies   | overall_remote_pct |
+---------------------+-----------------------+--------------------------+--------------------+
| 394300              | 138450.75             | 14280                    | 48.62              |
+---------------------+-----------------------+--------------------------+--------------------+
```

---

## 6. Screenshots Placeholder

| Scenario | Screenshot Reference | Description |
| :--- | :--- | :--- |
| **Hive Beeline Table Creation** | `[Screenshot: hive_beeline_ddl_success.png]` | Terminal showing successful execution of Star Schema DDL |
| **Hive Warehouse Query Output** | `[Screenshot: hive_query_results_table.png]` | Terminal showing tabular results of top skills and salary benchmarks |
| **Hive Metastore DB Inspection** | `[Screenshot: postgres_metastore_tables.png]` | PostgreSQL metastore relational table catalog entries |

---

## 7. Common Errors & Fixes

| Issue / Error | Root Cause | Solution / Fix |
| :--- | :--- | :--- |
| `Could not establish connection to jdbc:hive2://localhost:10000` | HiveServer2 is still initializing | HiveServer2 takes ~30-45 seconds to initialize on startup. Verify status: `docker logs hive-server`. |
| `SemanticException: Table not found` | Forgot `USE job_market_dw;` | Always set database context: `USE job_market_dw;` or specify fully qualified table names (`job_market_dw.fact_job_postings`). |
| `Failed to read Parquet data from HDFS` | Path typo or Gold ETL has not been run | Verify HDFS Gold layer exists: `docker exec -it namenode hdfs dfs -ls /data/job_market/gold`. |
