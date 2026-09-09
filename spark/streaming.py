"""
Spark Structured Streaming - Ingestion & Bronze Layer Pipeline
=============================================================
Consumes real-time job posting events from Apache Kafka ('job_postings'),
enforces strict schema validation, drops duplicates with watermarking,
extracts skill metadata, and continuously persists raw Bronze data to HDFS (Parquet).

Architecture Layer:
Kafka Topic ('job_postings') -> Spark Structured Streaming -> HDFS Bronze Layer (/data/job_market/bronze)
"""

import argparse
import logging
import os
import sys
from typing import Any, Dict, Optional
import yaml

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    current_timestamp,
    date_format,
    size,
    coalesce,
    lit,
    when,
    trim,
    lower
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
    ArrayType,
    TimestampType
)

# ------------------------------------------------------------------------------
# Logging Configuration
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("SparkStructuredStreaming")


# ------------------------------------------------------------------------------
# Kafka Ingestion JSON Schema
# ------------------------------------------------------------------------------
def get_job_postings_schema() -> StructType:
    """
    Returns the explicit PySpark schema matching the JSON payloads produced by kafka/producer.py.
    """
    return StructType([
        StructField("job_id", StringType(), nullable=False),
        StructField("company_name", StringType(), nullable=True),
        StructField("company_domain", StringType(), nullable=True),
        StructField("company_industry", StringType(), nullable=True),
        StructField("ats", StringType(), nullable=True),
        StructField("ats_token", StringType(), nullable=True),
        StructField("title", StringType(), nullable=True),
        StructField("department", StringType(), nullable=True),
        StructField("location_raw", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("is_remote", BooleanType(), nullable=True),
        StructField("employment_type", StringType(), nullable=True),
        StructField("salary_min", DoubleType(), nullable=True),
        StructField("salary_max", DoubleType(), nullable=True),
        StructField("salary_currency", StringType(), nullable=True),
        StructField("salary_period", StringType(), nullable=True),
        StructField("salary_from_text", BooleanType(), nullable=True),
        StructField("skills", ArrayType(StringType()), nullable=True),
        StructField("posted_at", StringType(), nullable=True),
        StructField("scraped_at", StringType(), nullable=True),
        StructField("url", StringType(), nullable=True),
        StructField("ingestion_timestamp", StringType(), nullable=True)
    ])


class SparkBronzeStreamer:
    """
    Manages the Spark Structured Streaming pipeline for the Bronze ingestion layer.
    """

    def __init__(self, config_path: str = "config/config.yaml", is_local: bool = True):
        self.config = self._load_config(config_path)
        self.is_local = is_local

        # Spark Settings
        spark_cfg = self.config.get("spark", {})
        self.app_name = spark_cfg.get("app_name", "JobMarketBronzeIngestion")
        self.master = spark_cfg.get("local_master", "local[*]") if is_local else spark_cfg.get("master", "spark://localhost:7077")
        self.trigger_seconds = spark_cfg.get("streaming_trigger_seconds", 10)

        # Kafka Settings
        kafka_cfg = self.config.get("kafka", {})
        self.kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", kafka_cfg.get("bootstrap_servers", "localhost:9092"))
        self.kafka_topic = kafka_cfg.get("topic", "job_postings")
        self.starting_offsets = kafka_cfg.get("auto_offset_reset", "earliest")

        # HDFS & Storage Settings
        hdfs_cfg = self.config.get("hdfs", {})
        namenode = hdfs_cfg.get("namenode_url", "hdfs://localhost:9000")
        
        # In case HDFS is accessed via cluster vs local fallback
        if is_local and os.getenv("USE_LOCAL_FS", "false").lower() == "true":
            self.bronze_output_path = "data/bronze"
            self.checkpoint_path = "data/checkpoints/bronze"
        else:
            self.bronze_output_path = f"{namenode}{hdfs_cfg.get('bronze_path', '/data/job_market/bronze')}"
            self.checkpoint_path = f"{namenode}{hdfs_cfg.get('checkpoint_path', '/data/job_market/checkpoints')}/bronze"

        # Initialize Spark Session
        self.spark = self._init_spark_session()

    @staticmethod
    def _load_config(path: str) -> Dict[str, Any]:
        """Loads pipeline YAML configuration file."""
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        return {}

    def _init_spark_session(self) -> SparkSession:
        """
        Builds SparkSession with Kafka streaming dependencies, HDFS configs, and optimized memory allocations.
        """
        logger.info("Initializing Spark Session (Master: %s)...", self.master)
        
        # Spark package coordinates for Kafka SQL integration
        # Compatible with Spark 3.5.0
        kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

        builder = (
            SparkSession.builder
            .appName(self.app_name)
            .master(self.master)
            .config("spark.jars.packages", kafka_package)
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
            .config("spark.sql.shuffle.partitions", "4")  # Optimized for streaming micro-batches
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.sql.session.timeZone", "UTC")
        )

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark Session established successfully. Spark Version: %s", spark.version)
        return spark

    def create_kafka_stream(self):
        """
        Establishes continuous streaming source from Apache Kafka.
        """
        logger.info("Connecting to Kafka topic '%s' at %s ...", self.kafka_topic, self.kafka_bootstrap)
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap)
            .option("subscribe", self.kafka_topic)
            .option("startingOffsets", self.starting_offsets)
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", 500)  # Rate limiting per micro-batch
            .load()
        )

    def process_stream(self, kafka_df):
        """
        Applies data cleaning, schema parsing, duplicate dropping with watermarking,
        and skill array enrichment.
        """
        json_schema = get_job_postings_schema()

        # Step 1: Deserialize Kafka Binary Value to JSON Schema
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), json_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")

        # Step 2: Validate essential columns (drop corrupt/empty payloads)
        valid_df = parsed_df.filter(
            (col("job_id").isNotNull()) & (trim(col("job_id")) != "") &
            (col("title").isNotNull()) & (trim(col("title")) != "")
        )

        # Step 3: Parse and standardize timestamps
        timestamped_df = (
            valid_df
            .withColumn("posted_time", coalesce(to_timestamp(col("posted_at")), col("kafka_timestamp")))
            .withColumn("ingestion_time", coalesce(to_timestamp(col("ingestion_timestamp")), current_timestamp()))
            .withColumn("processing_time", current_timestamp())
        )

        # Step 4: Watermarking & Duplicate Removal
        # Watermark on ingestion_time allows dropping duplicated job IDs within a 10-minute window
        deduped_df = (
            timestamped_df
            .withWatermark("ingestion_time", "10 minutes")
            .dropDuplicates(["job_id"])
        )

        # Step 5: Feature & Metadata Enrichment for Bronze Storage
        enriched_df = (
            deduped_df
            .withColumn("skills_count", size(coalesce(col("skills"), lit([]))))
            .withColumn("has_salary_info", when(col("salary_min").isNotNull() | col("salary_max").isNotNull(), True).otherwise(False))
            .withColumn("ingest_year", date_format(col("ingestion_time"), "yyyy"))
            .withColumn("ingest_month", date_format(col("ingestion_time"), "MM"))
            .withColumn("ingest_day", date_format(col("ingestion_time"), "dd"))
        )

        return enriched_df

    def start_streaming(self, console_preview: bool = False):
        """
        Starts the streaming query writing into HDFS Parquet Bronze layer.
        """
        kafka_df = self.create_kafka_stream()
        processed_df = self.process_stream(kafka_df)

        logger.info("==================================================")
        logger.info("Starting Spark Bronze Streaming Pipeline")
        logger.info("Source Kafka Topic: %s", self.kafka_topic)
        logger.info("Bronze Sink Path  : %s", self.bronze_output_path)
        logger.info("Checkpoint Path   : %s", self.checkpoint_path)
        logger.info("Batch Trigger     : %d seconds", self.trigger_seconds)
        logger.info("==================================================")

        queries = []

        # Stream 1: Parquet Write to HDFS Bronze Layer (Partitioned by Year/Month/Day)
        hdfs_query = (
            processed_df.writeStream
            .format("parquet")
            .outputMode("append")
            .option("path", self.bronze_output_path)
            .option("checkpointLocation", self.checkpoint_path)
            .partitionBy("ingest_year", "ingest_month", "ingest_day")
            .trigger(processingTime=f"{self.trigger_seconds} seconds")
            .queryName("bronze_hdfs_ingestion")
            .start()
        )
        queries.append(hdfs_query)

        # Stream 2 (Optional): Console Live Preview of micro-batch metrics
        if console_preview:
            console_query = (
                processed_df
                .select(
                    "job_id",
                    "company_name",
                    "title",
                    "city",
                    "is_remote",
                    "salary_min",
                    "salary_max",
                    "skills_count",
                    "ingestion_time"
                )
                .writeStream
                .format("console")
                .outputMode("append")
                .option("truncate", "false")
                .option("numRows", "10")
                .trigger(processingTime=f"{self.trigger_seconds} seconds")
                .queryName("console_debug_preview")
                .start()
            )
            queries.append(console_query)

        logger.info("Streaming queries active. Awaiting new events...")
        try:
            for q in queries:
                q.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Received interrupt. Stopping streaming queries gracefully...")
            for q in queries:
                q.stop()
            self.spark.stop()
            logger.info("Spark Structured Streaming pipeline terminated.")


# ------------------------------------------------------------------------------
# CLI Entrypoint
# ------------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Spark Structured Streaming pipeline from Kafka to HDFS Bronze layer."
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
        help="Run in local Spark mode (default: True)"
    )
    parser.add_argument(
        "--console",
        action="store_true",
        help="Output live micro-batch data preview to terminal console"
    )

    args = parser.parse_args()

    streamer = SparkBronzeStreamer(
        config_path=args.config,
        is_local=args.local
    )
    streamer.start_streaming(console_preview=args.console)


if __name__ == "__main__":
    main()
