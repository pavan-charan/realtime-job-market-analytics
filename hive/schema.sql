-- ==============================================================================
-- Apache Hive Data Warehouse DDL - Job Market Intelligence
-- Database: job_market_dw
-- Architecture: Kimball Star Schema (Parquet format backed by HDFS Gold Layer)
-- ==============================================================================

CREATE DATABASE IF NOT EXISTS job_market_dw
COMMENT 'Enterprise Lakehouse Data Warehouse for Real-Time Job Market Intelligence'
LOCATION '/data/job_market/hive_warehouse';

USE job_market_dw;

-- ------------------------------------------------------------------------------
-- 1. DIMENSION TABLES
-- ------------------------------------------------------------------------------

-- Dim Company: Descriptive attributes of hiring organizations
DROP TABLE IF EXISTS dim_company;
CREATE EXTERNAL TABLE IF NOT EXISTS dim_company (
    company_key BIGINT COMMENT 'Surrogate Primary Key',
    company_name STRING COMMENT 'Normalized Company Name',
    company_domain STRING COMMENT 'Corporate Web Domain',
    company_industry STRING COMMENT 'Industry Sector',
    total_jobs_posted BIGINT COMMENT 'Historical Total Job Volume'
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/job_market/gold/dim_company'
TBLPROPERTIES ('parquet.compress'='SNAPPY');


-- Dim Job Role: Standardized tech job role classifications and seniority levels
DROP TABLE IF EXISTS dim_job_role;
CREATE EXTERNAL TABLE IF NOT EXISTS dim_job_role (
    role_key BIGINT COMMENT 'Surrogate Primary Key',
    role_category STRING COMMENT 'Standardized Domain (Data Engineering, AI, etc.)',
    experience_level STRING COMMENT 'Seniority Level (Entry, Mid, Senior, Lead)'
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/job_market/gold/dim_job_role'
TBLPROPERTIES ('parquet.compress'='SNAPPY');


-- Dim Location: Geographic and workplace model attributes
DROP TABLE IF EXISTS dim_location;
CREATE EXTERNAL TABLE IF NOT EXISTS dim_location (
    location_key BIGINT COMMENT 'Surrogate Primary Key',
    city STRING COMMENT 'Normalized City Name',
    country STRING COMMENT 'Normalized Country Name',
    is_remote BOOLEAN COMMENT 'Remote Workplace Flag'
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/job_market/gold/dim_location'
TBLPROPERTIES ('parquet.compress'='SNAPPY');


-- Dim Date: Time dimension for longitudinal analysis
DROP TABLE IF EXISTS dim_date;
CREATE EXTERNAL TABLE IF NOT EXISTS dim_date (
    date_key INT COMMENT 'Surrogate Date Key in YYYYMMDD format',
    full_date DATE COMMENT 'Calendar Date',
    year INT COMMENT 'Calendar Year',
    month INT COMMENT 'Month Number (1-12)',
    day INT COMMENT 'Day of Month (1-31)',
    quarter INT COMMENT 'Quarter of Year (1-4)',
    day_name STRING COMMENT 'Day of Week Name',
    month_name STRING COMMENT 'Month Name'
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/job_market/gold/dim_date'
TBLPROPERTIES ('parquet.compress'='SNAPPY');


-- Dim Skill: Normalized technology and programming skills catalogue
DROP TABLE IF EXISTS dim_skill;
CREATE EXTERNAL TABLE IF NOT EXISTS dim_skill (
    skill_key BIGINT COMMENT 'Surrogate Primary Key',
    skill_name STRING COMMENT 'Normalized lowercase skill tag'
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/job_market/gold/dim_skill'
TBLPROPERTIES ('parquet.compress'='SNAPPY');


-- Bridge Table: Many-to-Many Relationship between Job Postings and Skills
DROP TABLE IF EXISTS bridge_job_skills;
CREATE EXTERNAL TABLE IF NOT EXISTS bridge_job_skills (
    job_id STRING COMMENT 'Foreign Key referencing fact_job_postings',
    skill_key BIGINT COMMENT 'Foreign Key referencing dim_skill'
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/job_market/gold/bridge_job_skills'
TBLPROPERTIES ('parquet.compress'='SNAPPY');


-- ------------------------------------------------------------------------------
-- 2. FACT TABLE
-- ------------------------------------------------------------------------------

-- Fact Job Postings: Central metric and transaction table
DROP TABLE IF EXISTS fact_job_postings;
CREATE EXTERNAL TABLE IF NOT EXISTS fact_job_postings (
    fact_key BIGINT COMMENT 'Surrogate Fact Key',
    job_id STRING COMMENT 'Natural Unique Job Posting Identifier',
    company_key BIGINT COMMENT 'Foreign Key to dim_company',
    role_key BIGINT COMMENT 'Foreign Key to dim_job_role',
    location_key BIGINT COMMENT 'Foreign Key to dim_location',
    date_key INT COMMENT 'Foreign Key to dim_date',
    title STRING COMMENT 'Raw Job Title',
    annual_salary_usd DOUBLE COMMENT 'Normalized Annualized Salary in USD',
    salary_min DOUBLE COMMENT 'Minimum Offered Salary',
    salary_max DOUBLE COMMENT 'Maximum Offered Salary',
    salary_currency STRING COMMENT 'Original Salary Currency',
    employment_type STRING COMMENT 'Full-time, Part-time, Contract, etc.',
    is_remote BOOLEAN COMMENT 'Remote Indicator',
    skills_count INT COMMENT 'Count of Tagged Skills',
    url STRING COMMENT 'Original Job Application URL'
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/job_market/gold/fact_job_postings'
TBLPROPERTIES ('parquet.compress'='SNAPPY');


-- ------------------------------------------------------------------------------
-- 3. PRE-AGGREGATED GOLD ANALYTICS TABLES (High-Speed Grafana Sinks)
-- ------------------------------------------------------------------------------

-- Gold Top Skills
DROP TABLE IF EXISTS gold_top_skills;
CREATE EXTERNAL TABLE IF NOT EXISTS gold_top_skills (
    skill_name STRING,
    posting_count BIGINT,
    avg_salary_usd DOUBLE,
    remote_percentage DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/job_market/gold/gold_top_skills';


-- Gold Salary by Role & City
DROP TABLE IF EXISTS gold_salary_by_role_city;
CREATE EXTERNAL TABLE IF NOT EXISTS gold_salary_by_role_city (
    role_category STRING,
    city STRING,
    experience_level STRING,
    total_postings BIGINT,
    avg_annual_salary DOUBLE,
    min_annual_salary DOUBLE,
    max_annual_salary DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/job_market/gold/gold_salary_by_role_city';


-- Gold Remote Work Trends
DROP TABLE IF EXISTS gold_remote_trends;
CREATE EXTERNAL TABLE IF NOT EXISTS gold_remote_trends (
    role_category STRING,
    is_remote BOOLEAN,
    total_jobs BIGINT,
    avg_salary DOUBLE,
    avg_skills_required DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/job_market/gold/gold_remote_trends';


-- Gold Company Hiring Analytics
DROP TABLE IF EXISTS gold_company_hiring;
CREATE EXTERNAL TABLE IF NOT EXISTS gold_company_hiring (
    company_name STRING,
    company_industry STRING,
    open_positions BIGINT,
    avg_offered_salary DOUBLE,
    pct_remote DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/job_market/gold/gold_company_hiring';


-- ------------------------------------------------------------------------------
-- 4. SPARK MLLIB PREDICTION RESULTS TABLE (Phase 6 Sink)
-- ------------------------------------------------------------------------------

DROP TABLE IF EXISTS prediction_results;
CREATE TABLE IF NOT EXISTS prediction_results (
    prediction_id STRING COMMENT 'Unique Prediction UUID',
    role_category STRING COMMENT 'Selected Job Role',
    experience_level STRING COMMENT 'Selected Experience Level',
    city STRING COMMENT 'Selected City',
    is_remote BOOLEAN COMMENT 'Remote Flag',
    skills_count INT COMMENT 'Number of Skills',
    primary_skill STRING COMMENT 'Primary High-Demand Skill',
    predicted_annual_salary DOUBLE COMMENT 'Spark MLlib Model Predicted Salary (USD)',
    confidence_score DOUBLE COMMENT 'Model Confidence (R2 / Variance Metric)',
    prediction_timestamp TIMESTAMP COMMENT 'Inference Execution Timestamp'
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/job_market/gold/prediction_results';
