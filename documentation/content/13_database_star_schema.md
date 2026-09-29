# 13. Database Design & Hive Star Schema

The data warehouse is structured as a dimensional **Star Schema** optimized for high-performance analytical queries.

### Dimensional Model Overview
- **Fact Table:** `fact_job_postings` (Grain: One row per unique job posting).
- **Dimension Tables:**
  - `dim_company`: Company attributes, industry classification.
  - `dim_job_role`: Standardized role categories and seniority tiers.
  - `dim_location`: City, country, and remote work policy.
  - `dim_skill`: Standardized technical skills.
  - `dim_date`: Calendar dimension for time-series analytics.
  - `bridge_job_skills`: Many-to-many bridge mapping job postings to multi-skill lists.
- **Aggregated Gold Tables:** `gold_top_skills`, `gold_salary_by_role_city`, `gold_company_hiring`, `gold_remote_trends`.

---

### Table: `dim_company`
- **Format:** PARQUET
- **Partitioning:** None

| Column & Type |
| :--- |
| company_key BIGINT COMMENT 'Surrogate Primary Key'<br>company_name STRING COMMENT 'Normalized Company Name'<br>company_domain STRING COMMENT 'Corporate Web Domain'<br>company_industry STRING COMMENT 'Industry Sector'<br>total_jobs_posted BIGINT COMMENT 'Historical Total Job Volume' |

### Table: `dim_job_role`
- **Format:** PARQUET
- **Partitioning:** None

| Column & Type |
| :--- |
| role_key BIGINT COMMENT 'Surrogate Primary Key'<br>role_category STRING COMMENT 'Standardized Domain (Data Engineering, AI, etc.)'<br>experience_level STRING COMMENT 'Seniority Level (Entry, Mid, Senior, Lead)' |

### Table: `dim_location`
- **Format:** PARQUET
- **Partitioning:** None

| Column & Type |
| :--- |
| location_key BIGINT COMMENT 'Surrogate Primary Key'<br>city STRING COMMENT 'Normalized City Name'<br>country STRING COMMENT 'Normalized Country Name'<br>is_remote BOOLEAN COMMENT 'Remote Workplace Flag' |

### Table: `dim_date`
- **Format:** PARQUET
- **Partitioning:** None

| Column & Type |
| :--- |
| date_key INT COMMENT 'Surrogate Date Key in YYYYMMDD format'<br>full_date DATE COMMENT 'Calendar Date'<br>year INT COMMENT 'Calendar Year'<br>month INT COMMENT 'Month Number (1-12)'<br>day INT COMMENT 'Day of Month (1-31)'<br>quarter INT COMMENT 'Quarter of Year (1-4)'<br>day_name STRING COMMENT 'Day of Week Name'<br>month_name STRING COMMENT 'Month Name' |

### Table: `dim_skill`
- **Format:** PARQUET
- **Partitioning:** None

| Column & Type |
| :--- |
| skill_key BIGINT COMMENT 'Surrogate Primary Key'<br>skill_name STRING COMMENT 'Normalized lowercase skill tag' |

### Table: `bridge_job_skills`
- **Format:** PARQUET
- **Partitioning:** None

| Column & Type |
| :--- |
| job_id STRING COMMENT 'Foreign Key referencing fact_job_postings'<br>skill_key BIGINT COMMENT 'Foreign Key referencing dim_skill' |

### Table: `fact_job_postings`
- **Format:** PARQUET
- **Partitioning:** None

| Column & Type |
| :--- |
| fact_key BIGINT COMMENT 'Surrogate Fact Key'<br>job_id STRING COMMENT 'Natural Unique Job Posting Identifier'<br>company_key BIGINT COMMENT 'Foreign Key to dim_company'<br>role_key BIGINT COMMENT 'Foreign Key to dim_job_role'<br>location_key BIGINT COMMENT 'Foreign Key to dim_location'<br>date_key INT COMMENT 'Foreign Key to dim_date'<br>title STRING COMMENT 'Raw Job Title'<br>annual_salary_usd DOUBLE COMMENT 'Normalized Annualized Salary in USD'<br>salary_min DOUBLE COMMENT 'Minimum Offered Salary'<br>salary_max DOUBLE COMMENT 'Maximum Offered Salary'<br>salary_currency STRING COMMENT 'Original Salary Currency'<br>employment_type STRING COMMENT 'Full-time, Part-time, Contract, etc.'<br>is_remote BOOLEAN COMMENT 'Remote Indicator'<br>skills_count INT COMMENT 'Count of Tagged Skills'<br>url STRING COMMENT 'Original Job Application URL' |

### Table: `gold_top_skills`
- **Format:** PARQUET
- **Partitioning:** None

| Column & Type |
| :--- |
| skill_name STRING<br>posting_count BIGINT<br>avg_salary_usd DOUBLE<br>remote_percentage DOUBLE |

### Table: `gold_salary_by_role_city`
- **Format:** PARQUET
- **Partitioning:** None

| Column & Type |
| :--- |
| role_category STRING<br>city STRING<br>experience_level STRING<br>total_postings BIGINT<br>avg_annual_salary DOUBLE<br>min_annual_salary DOUBLE<br>max_annual_salary DOUBLE |

### Table: `gold_remote_trends`
- **Format:** PARQUET
- **Partitioning:** None

| Column & Type |
| :--- |
| role_category STRING<br>is_remote BOOLEAN<br>total_jobs BIGINT<br>avg_salary DOUBLE<br>avg_skills_required DOUBLE |

### Table: `gold_company_hiring`
- **Format:** PARQUET
- **Partitioning:** None

| Column & Type |
| :--- |
| company_name STRING<br>company_industry STRING<br>open_positions BIGINT<br>avg_offered_salary DOUBLE<br>pct_remote DOUBLE |

### Table: `prediction_results`
- **Format:** PARQUET
- **Partitioning:** None

| Column & Type |
| :--- |
| prediction_id STRING COMMENT 'Unique Prediction UUID'<br>role_category STRING COMMENT 'Selected Job Role'<br>experience_level STRING COMMENT 'Selected Experience Level'<br>city STRING COMMENT 'Selected City'<br>is_remote BOOLEAN COMMENT 'Remote Flag'<br>skills_count INT COMMENT 'Number of Skills'<br>primary_skill STRING COMMENT 'Primary High-Demand Skill'<br>predicted_annual_salary DOUBLE COMMENT 'Spark MLlib Model Predicted Salary (USD)'<br>confidence_score DOUBLE COMMENT 'Model Confidence (R2 / Variance Metric)'<br>prediction_timestamp TIMESTAMP COMMENT 'Inference Execution Timestamp' |
