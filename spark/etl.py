"""
Spark SQL ETL Pipeline - Medallion Architecture (Bronze -> Silver -> Gold)
==========================================================================
Transforms raw Bronze streaming data into cleansed Silver data,
and aggregates Silver data into Gold Star-Schema Facts, Dimensions,
and Analytics Tables ready for Hive Data Warehouse and Spark MLlib.

Medallion Flow:
  [ HDFS Bronze: /data/job_market/bronze ]
                     │
                     ▼ (ETL 1: Cleaning, Normalization, Salary Annualization, Role Classification)
  [ HDFS Silver: /data/job_market/silver ]
                     │
                     ▼ (ETL 2: Star Schema Modeling & Analytical Aggregations)
  [ HDFS Gold: /data/job_market/gold ]
     ├── fact_job_postings
     ├── dim_company, dim_job_role, dim_location, dim_date, dim_skill, bridge_job_skills
     └── Analytical Aggregates (gold_top_skills, gold_salary_by_role_city, gold_remote_trends)
"""

import argparse
import logging
import os
import sys
from typing import Any, Dict
import yaml

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    upper,
    initcap,
    coalesce,
    when,
    lit,
    round as spark_round,
    avg,
    count,
    min as spark_min,
    max as spark_max,
    explode,
    to_date,
    year,
    month,
    dayofmonth,
    quarter,
    date_format,
    monotonically_increasing_id,
    regexp_replace,
    split,
    size
)

# ------------------------------------------------------------------------------
# Logging Setup
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("SparkSQLETL")


class JobMarketETL:
    """
    Executes Medallion Architecture transformations across Bronze, Silver, and Gold layers.
    """

    def __init__(self, config_path: str = "config/config.yaml", is_local: bool = True):
        self.config = self._load_config(config_path)
        self.is_local = is_local

        # Spark Settings
        spark_cfg = self.config.get("spark", {})
        self.app_name = spark_cfg.get("app_name", "JobMarketLakehouseETL")
        self.master = spark_cfg.get("local_master", "local[*]") if is_local else spark_cfg.get("master", "spark://localhost:7077")

        # Storage Paths
        hdfs_cfg = self.config.get("hdfs", {})
        namenode = hdfs_cfg.get("namenode_url", "hdfs://localhost:9000")

        if is_local and os.getenv("USE_LOCAL_FS", "false").lower() == "true":
            self.bronze_path = "data/bronze"
            self.silver_path = "data/silver"
            self.gold_path = "data/gold"
        else:
            self.bronze_path = f"{namenode}{hdfs_cfg.get('bronze_path', '/data/job_market/bronze')}"
            self.silver_path = f"{namenode}{hdfs_cfg.get('silver_path', '/data/job_market/silver')}"
            self.gold_path = f"{namenode}{hdfs_cfg.get('gold_path', '/data/job_market/gold')}"

        self.spark = self._init_spark_session()

    @staticmethod
    def _load_config(path: str) -> Dict[str, Any]:
        """Loads YAML configuration file."""
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        return {}

    def _init_spark_session(self) -> SparkSession:
        """Initializes SparkSession configured for high-performance Spark SQL operations."""
        logger.info("Initializing Spark SQL Session (Master: %s)...", self.master)
        spark = (
            SparkSession.builder
            .appName(self.app_name)
            .master(self.master)
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark Session established. Version: %s", spark.version)
        return spark

    # ==========================================================================
    # Phase 4A: Bronze -> Silver Transformation
    # ==========================================================================
    def run_bronze_to_silver(self) -> DataFrame:
        """
        Cleans, normalizes, deduplicates, and enriches raw Bronze Parquet data into Silver layer.
        """
        logger.info("==================================================")
        logger.info("Starting Bronze -> Silver Transformation")
        logger.info("Reading Bronze data from: %s", self.bronze_path)
        logger.info("==================================================")

        # 1. Read Bronze Parquet (or fallback to CSV dataset for full ETL if Bronze is empty)
        try:
            bronze_df = self.spark.read.parquet(self.bronze_path)
            record_count = bronze_df.count()
            logger.info("Loaded %d records from Bronze Parquet.", record_count)
            if record_count == 0:
                raise ValueError("Bronze dataset is empty.")
        except Exception as e:
            logger.warning("Could not read Bronze Parquet (%s). Fallback reading dataset/tech_job_postings.csv...", e)
            bronze_df = (
                self.spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("dataset/tech_job_postings.csv")
                .withColumn("skills", split(col("skills"), r"\|"))
                .withColumn("ingestion_timestamp", col("scraped_at"))
            )
            logger.info("Loaded %d records directly from raw CSV fallback.", bronze_df.count())

        # Register Temp View for Spark SQL
        bronze_df.createOrReplaceTempView("raw_bronze_postings")

        # 2. Spark SQL Transformation & Cleansing
        silver_df = self.spark.sql("""
            SELECT
                TRIM(job_id) AS job_id,
                TRIM(COALESCE(company_name, 'Confidential / Unknown')) AS company_name,
                TRIM(company_domain) AS company_domain,
                TRIM(COALESCE(company_industry, 'Technology')) AS company_industry,
                TRIM(ats) AS ats,
                TRIM(title) AS title,
                
                -- Standardized Job Role Category Classification
                CASE
                    WHEN LOWER(title) RLIKE 'data eng|data infra|spark|hadoop|etl|pipeline' THEN 'Data Engineering'
                    WHEN LOWER(title) RLIKE 'data sci|machine learn|ai |deep learn|nlp|computer vision' THEN 'Data Science & AI'
                    WHEN LOWER(title) RLIKE 'data anal|bi |business intell|analytics' THEN 'Data Analytics & BI'
                    WHEN LOWER(title) RLIKE 'devops|sre|cloud|infrastructure|platform' THEN 'DevOps & Cloud'
                    WHEN LOWER(title) RLIKE 'backend|software eng|full stack|java |golang|python|c\\+\\+' THEN 'Software Engineering'
                    WHEN LOWER(title) RLIKE 'frontend|react|vue|angular|web dev|ui|ux' THEN 'Frontend & Web'
                    WHEN LOWER(title) RLIKE 'product man|scrum|agile|program man' THEN 'Product Management'
                    WHEN LOWER(title) RLIKE 'security|cyber|infosec' THEN 'Cybersecurity'
                    ELSE 'Other Tech Roles'
                END AS role_category,

                -- Experience Level Derivation
                CASE
                    WHEN LOWER(title) RLIKE 'lead|principal|staff|architect|director|head|vp|chief' THEN 'Lead / Principal'
                    WHEN LOWER(title) RLIKE 'senior|sr|sr\\.' THEN 'Senior'
                    WHEN LOWER(title) RLIKE 'junior|jr|jr\\.|associate|intern|entry|grad' THEN 'Entry / Junior'
                    ELSE 'Mid-Level'
                END AS experience_level,

                TRIM(department) AS department,
                
                -- Location Normalization
                CASE
                    WHEN is_remote = true OR LOWER(location_raw) RLIKE 'remote' THEN 'Remote'
                    WHEN city IS NOT NULL AND TRIM(city) != '' THEN INITCAP(TRIM(city))
                    WHEN country IS NOT NULL AND TRIM(country) != '' THEN INITCAP(TRIM(country))
                    ELSE 'Remote'
                END AS city_normalized,

                COALESCE(INITCAP(TRIM(country)), 'Global') AS country_normalized,
                COALESCE(is_remote, false) AS is_remote,
                COALESCE(LOWER(TRIM(employment_type)), 'full_time') AS employment_type,

                -- Salary Normalization & Imputation
                CAST(salary_min AS DOUBLE) AS salary_min,
                CAST(salary_max AS DOUBLE) AS salary_max,
                COALESCE(UPPER(TRIM(salary_currency)), 'USD') AS salary_currency,
                COALESCE(LOWER(TRIM(salary_period)), 'year') AS salary_period,

                -- Annualized Standard Salary Calculation
                CASE
                    WHEN salary_min IS NOT NULL AND salary_max IS NOT NULL THEN
                        ROUND(
                            CASE
                                WHEN LOWER(salary_period) = 'hour' THEN ((salary_min + salary_max) / 2.0) * 2080
                                WHEN LOWER(salary_period) = 'month' THEN ((salary_min + salary_max) / 2.0) * 12
                                WHEN LOWER(salary_period) = 'week' THEN ((salary_min + salary_max) / 2.0) * 52
                                WHEN LOWER(salary_period) = 'day' THEN ((salary_min + salary_max) / 2.0) * 260
                                ELSE ((salary_min + salary_max) / 2.0)
                            END, 2
                        )
                    WHEN salary_min IS NOT NULL THEN
                        ROUND(
                            CASE
                                WHEN LOWER(salary_period) = 'hour' THEN salary_min * 2080
                                WHEN LOWER(salary_period) = 'month' THEN salary_min * 12
                                ELSE salary_min
                            END, 2
                        )
                    WHEN salary_max IS NOT NULL THEN
                        ROUND(
                            CASE
                                WHEN LOWER(salary_period) = 'hour' THEN salary_max * 2080
                                WHEN LOWER(salary_period) = 'month' THEN salary_max * 12
                                ELSE salary_max
                            END, 2
                        )
                    ELSE NULL
                END AS annual_salary_usd,

                skills,
                SIZE(COALESCE(skills, ARRAY())) AS skills_count,
                TO_DATE(COALESCE(posted_at, ingestion_timestamp)) AS posted_date,
                COALESCE(posted_at, ingestion_timestamp) AS posted_at_raw,
                url
            FROM raw_bronze_postings
            WHERE job_id IS NOT NULL AND TRIM(job_id) != ''
        """)

        # 3. Deduplicate by job_id
        silver_cleaned = silver_df.dropDuplicates(["job_id"])

        # Filter out extreme salary anomalies (< $10k or > $1.5M annually)
        silver_final = silver_cleaned.withColumn(
            "annual_salary_usd",
            when((col("annual_salary_usd") >= 10000.0) & (col("annual_salary_usd") <= 1500000.0), col("annual_salary_usd"))
            .otherwise(lit(None))
        )

        logger.info("Writing Silver layer to Parquet at: %s", self.silver_path)
        (
            silver_final.write
            .mode("overwrite")
            .parquet(self.silver_path)
        )
        logger.info("Successfully wrote %d cleansed records to Silver layer.", silver_final.count())
        return silver_final

    # ==========================================================================
    # Phase 4B: Silver -> Gold Transformation (Star Schema & Analytical Tables)
    # ==========================================================================
    def run_silver_to_gold(self, silver_df: DataFrame = None):
        """
        Transforms cleansed Silver data into Gold Star Schema Facts, Dimensions,
        and Pre-aggregated Business Analytics tables.
        """
        logger.info("==================================================")
        logger.info("Starting Silver -> Gold Transformation")
        logger.info("==================================================")

        if silver_df is None:
            logger.info("Reading Silver data from: %s", self.silver_path)
            silver_df = self.spark.read.parquet(self.silver_path)

        silver_df.createOrReplaceTempView("silver_postings")

        # ----------------------------------------------------------------------
        # 1. Dimension Tables
        # ----------------------------------------------------------------------
        logger.info("Generating Star Schema Dimension Tables...")

        # Dim Company
        dim_company = self.spark.sql("""
            SELECT
                ROW_NUMBER() OVER (ORDER BY company_name) AS company_key,
                company_name,
                FIRST(company_domain) AS company_domain,
                FIRST(company_industry) AS company_industry,
                COUNT(job_id) AS total_jobs_posted
            FROM silver_postings
            GROUP BY company_name
        """)
        dim_company.write.mode("overwrite").parquet(f"{self.gold_path}/dim_company")
        dim_company.createOrReplaceTempView("dim_company")

        # Dim Job Role
        dim_job_role = self.spark.sql("""
            SELECT
                ROW_NUMBER() OVER (ORDER BY role_category, experience_level) AS role_key,
                role_category,
                experience_level
            FROM (SELECT DISTINCT role_category, experience_level FROM silver_postings)
        """)
        dim_job_role.write.mode("overwrite").parquet(f"{self.gold_path}/dim_job_role")
        dim_job_role.createOrReplaceTempView("dim_job_role")

        # Dim Location
        dim_location = self.spark.sql("""
            SELECT
                ROW_NUMBER() OVER (ORDER BY city_normalized, country_normalized) AS location_key,
                city_normalized AS city,
                country_normalized AS country,
                is_remote
            FROM (SELECT DISTINCT city_normalized, country_normalized, is_remote FROM silver_postings)
        """)
        dim_location.write.mode("overwrite").parquet(f"{self.gold_path}/dim_location")
        dim_location.createOrReplaceTempView("dim_location")

        # Dim Date
        dim_date = self.spark.sql("""
            SELECT
                CAST(DATE_FORMAT(posted_date, 'yyyyMMdd') AS INT) AS date_key,
                posted_date AS full_date,
                YEAR(posted_date) AS year,
                MONTH(posted_date) AS month,
                DAY(posted_date) AS day,
                QUARTER(posted_date) AS quarter,
                DATE_FORMAT(posted_date, 'EEEE') AS day_name,
                DATE_FORMAT(posted_date, 'MMMM') AS month_name
            FROM (SELECT DISTINCT posted_date FROM silver_postings WHERE posted_date IS NOT NULL)
        """)
        dim_date.write.mode("overwrite").parquet(f"{self.gold_path}/dim_date")
        dim_date.createOrReplaceTempView("dim_date")

        # Dim Skill
        dim_skill = (
            silver_df
            .select(explode(col("skills")).alias("skill_name"))
            .filter(col("skill_name").isNotNull() & (trim(col("skill_name")) != ""))
            .withColumn("skill_name", lower(trim(col("skill_name"))))
            .distinct()
            .withColumn("skill_key", monotonically_increasing_id() + 1)
        )
        dim_skill.write.mode("overwrite").parquet(f"{self.gold_path}/dim_skill")
        dim_skill.createOrReplaceTempView("dim_skill")

        # ----------------------------------------------------------------------
        # 2. Fact Table (fact_job_postings)
        # ----------------------------------------------------------------------
        logger.info("Generating Star Schema Fact Table (fact_job_postings)...")
        fact_job_postings = self.spark.sql("""
            SELECT
                ROW_NUMBER() OVER (ORDER BY s.posted_date, s.job_id) AS fact_key,
                s.job_id,
                c.company_key,
                r.role_key,
                l.location_key,
                COALESCE(d.date_key, 19700101) AS date_key,
                s.title,
                s.annual_salary_usd,
                s.salary_min,
                s.salary_max,
                s.salary_currency,
                s.employment_type,
                s.is_remote,
                s.skills_count,
                s.url
            FROM silver_postings s
            LEFT JOIN dim_company c ON s.company_name = c.company_name
            LEFT JOIN dim_job_role r ON s.role_category = r.role_category AND s.experience_level = r.experience_level
            LEFT JOIN dim_location l ON s.city_normalized = l.city AND s.country_normalized = l.country AND s.is_remote = l.is_remote
            LEFT JOIN dim_date d ON s.posted_date = d.full_date
        """)
        fact_job_postings.write.mode("overwrite").parquet(f"{self.gold_path}/fact_job_postings")
        fact_job_postings.createOrReplaceTempView("fact_job_postings")

        # Bridge Table: Job to Skills (Many-to-Many)
        logger.info("Generating Bridge Table (bridge_job_skills)...")
        exploded_skills = silver_df.select(
            col("job_id"),
            explode(col("skills")).alias("skill_name")
        ).withColumn("skill_name", lower(trim(col("skill_name"))))
        
        bridge_job_skills = exploded_skills.join(dim_skill, "skill_name").select(
            col("job_id"),
            col("skill_key")
        )
        bridge_job_skills.write.mode("overwrite").parquet(f"{self.gold_path}/bridge_job_skills")

        # ----------------------------------------------------------------------
        # 3. Gold Analytical Aggregates for Grafana & Reporting
        # ----------------------------------------------------------------------
        logger.info("Generating Gold Analytical Aggregate Tables...")

        # Gold Aggregate 1: Top Skills Demand & Salaries
        gold_top_skills = self.spark.sql("""
            SELECT
                LOWER(TRIM(skill)) AS skill_name,
                COUNT(job_id) AS posting_count,
                ROUND(AVG(annual_salary_usd), 2) AS avg_salary_usd,
                ROUND(COUNT(CASE WHEN is_remote = true THEN 1 END) * 100.0 / COUNT(job_id), 2) AS remote_percentage
            FROM silver_postings
            LATERAL VIEW EXPLODE(skills) exploded_table AS skill
            WHERE skill IS NOT NULL AND TRIM(skill) != ''
            GROUP BY LOWER(TRIM(skill))
            ORDER BY posting_count DESC
        """)
        gold_top_skills.write.mode("overwrite").parquet(f"{self.gold_path}/gold_top_skills")

        # Gold Aggregate 2: Salary by Role Category and City
        gold_salary_by_role_city = self.spark.sql("""
            SELECT
                role_category,
                city_normalized AS city,
                experience_level,
                COUNT(job_id) AS total_postings,
                ROUND(AVG(annual_salary_usd), 2) AS avg_annual_salary,
                ROUND(MIN(annual_salary_usd), 2) AS min_annual_salary,
                ROUND(MAX(annual_salary_usd), 2) AS max_annual_salary
            FROM silver_postings
            WHERE annual_salary_usd IS NOT NULL
            GROUP BY role_category, city_normalized, experience_level
            ORDER BY total_postings DESC
        """)
        gold_salary_by_role_city.write.mode("overwrite").parquet(f"{self.gold_path}/gold_salary_by_role_city")

        # Gold Aggregate 3: Remote vs Onsite Hiring Trends
        gold_remote_trends = self.spark.sql("""
            SELECT
                role_category,
                is_remote,
                COUNT(job_id) AS total_jobs,
                ROUND(AVG(annual_salary_usd), 2) AS avg_salary,
                ROUND(AVG(skills_count), 2) AS avg_skills_required
            FROM silver_postings
            GROUP BY role_category, is_remote
            ORDER BY role_category, is_remote
        """)
        gold_remote_trends.write.mode("overwrite").parquet(f"{self.gold_path}/gold_remote_trends")

        # Gold Aggregate 4: Company Hiring Velocity
        gold_company_hiring = self.spark.sql("""
            SELECT
                company_name,
                company_industry,
                COUNT(job_id) AS open_positions,
                ROUND(AVG(annual_salary_usd), 2) AS avg_offered_salary,
                ROUND(COUNT(CASE WHEN is_remote = true THEN 1 END) * 100.0 / COUNT(job_id), 2) AS pct_remote
            FROM silver_postings
            GROUP BY company_name, company_industry
            ORDER BY open_positions DESC
        """)
        gold_company_hiring.write.mode("overwrite").parquet(f"{self.gold_path}/gold_company_hiring")

        logger.info("==================================================")
        logger.info("Successfully generated all Gold Star Schema & Analytics tables in HDFS!")
        logger.info("Fact Table Count: %d rows", fact_job_postings.count())
        logger.info("==================================================")


# ------------------------------------------------------------------------------
# CLI Entrypoint
# ------------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Spark SQL Medallion ETL (Bronze -> Silver -> Gold)"
    )
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to pipeline configuration YAML (default: config/config.yaml)"
    )
    parser.add_argument(
        "--local",
        action="store_true",
        default=True,
        help="Run locally with embedded Spark"
    )
    parser.add_argument(
        "--mode",
        choices=["all", "bronze-to-silver", "silver-to-gold"],
        default="all",
        help="ETL stage to execute: 'all', 'bronze-to-silver', or 'silver-to-gold'"
    )

    args = parser.parse_args()

    etl = JobMarketETL(config_path=args.config, is_local=args.local)

    if args.mode in ["all", "bronze-to-silver"]:
        silver_df = etl.run_bronze_to_silver()
    else:
        silver_df = None

    if args.mode in ["all", "silver-to-gold"]:
        etl.run_silver_to_gold(silver_df)


if __name__ == "__main__":
    main()
