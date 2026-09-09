"""
Kafka Producer for Real-Time Job Market Intelligence Pipeline
=============================================================
Reads historical job postings dataset (CSV), serializes rows into JSON,
and streams them to the Apache Kafka topic ('job_postings') at a controlled rate.

Features:
- Configurable streaming throughput (default: 10 events/second)
- JSON transformation with field parsing (skills array, boolean flags, numbers)
- Batching, retries, and delivery report callbacks
- Graceful shutdown handling (SIGINT/SIGTERM)
- Live streaming throughput & delivery metrics logging
"""

import argparse
import csv
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from typing import Any, Dict, Generator, Optional
import yaml
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# ------------------------------------------------------------------------------
# Logging Setup
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("KafkaProducer")


class JobPostingsProducer:
    """
    Simulates real-time ingestion by streaming historical job posting records
    into an Apache Kafka topic with configurable velocity and batching.
    """

    def __init__(self, config_path: str = "config/config.yaml", rate: Optional[float] = None, limit: Optional[int] = None):
        """
        Initialize the Producer with configuration from YAML and CLI overrides.

        Args:
            config_path (str): Path to config.yaml
            rate (float, optional): Events per second override. Defaults to config value or 10.0.
            limit (int, optional): Max records to send before stopping. None = stream entire file.
        """
        self.config = self._load_config(config_path)
        self.rate = rate if rate is not None else float(self.config.get("kafka", {}).get("producer_rate", 10.0))
        self.limit = limit
        self.running = True
        self.total_sent = 0
        self.total_acknowledged = 0
        self.total_failed = 0

        # Topic & Broker Settings
        kafka_cfg = self.config.get("kafka", {})
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", kafka_cfg.get("bootstrap_servers", "localhost:9092"))
        self.topic = kafka_cfg.get("topic", "job_postings")
        
        # Dataset Path
        dataset_cfg = self.config.get("dataset", {})
        self.csv_path = dataset_cfg.get("raw_csv_path", "dataset/tech_job_postings.csv")

        # Initialize Kafka Producer Client
        self.producer = self._create_producer()

        # Register termination signal handlers
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    @staticmethod
    def _load_config(path: str) -> Dict[str, Any]:
        """Loads YAML configuration file."""
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        logger.warning("Config file '%s' not found. Using default configurations.", path)
        return {}

    def _create_producer(self, retries: int = 10, backoff: float = 3.0) -> KafkaProducer:
        """
        Creates KafkaProducer instance with retry logic to wait for broker availability.
        """
        logger.info("Connecting to Kafka broker at: %s ...", self.bootstrap_servers)
        for attempt in range(1, retries + 1):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                    key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
                    acks="all",                      # Full replica acknowledgement for reliability
                    retries=3,                       # Internal producer retries
                    compression_type="gzip",         # Compress payloads for high throughput
                    linger_ms=10,                    # Small buffer time to batch events
                    max_in_flight_requests_per_connection=5
                )
                logger.info("Successfully connected to Kafka broker! Target Topic: '%s'", self.topic)
                return producer
            except NoBrokersAvailable as e:
                logger.warning("Attempt %d/%d: Kafka broker not ready (%s). Retrying in %.1fs...", attempt, retries, e, backoff)
                time.sleep(backoff)
            except Exception as e:
                logger.error("Unexpected error connecting to Kafka: %s", e)
                time.sleep(backoff)

        logger.critical("Could not connect to Kafka broker after %d attempts. Exiting.", retries)
        sys.exit(1)

    def _handle_shutdown(self, signum, frame):
        """Gracefully handle termination signals."""
        logger.info("Shutdown signal (%s) received. Flushing remaining messages...", signum)
        self.running = False

    def _on_send_success(self, record_metadata):
        """Callback for successful message delivery."""
        self.total_acknowledged += 1

    def _on_send_error(self, exc):
        """Callback for failed message delivery."""
        self.total_failed += 1
        logger.error("Failed to deliver message: %s", exc)

    @staticmethod
    def parse_job_row(row: Dict[str, str]) -> Dict[str, Any]:
        """
        Transform raw CSV row dictionary into a clean, typed JSON object.
        - Parses skill pipe-delimited string into list
        - Casts salary fields to float/None
        - Casts boolean flags
        - Adds ingestion timestamp
        """
        # Parse skills array
        skills_raw = row.get("skills", "")
        skills_list = [s.strip() for s in skills_raw.split("|") if s.strip()] if skills_raw else []

        # Parse numeric salaries
        def parse_float(val: Optional[str]) -> Optional[float]:
            if not val or val.strip().lower() in ("", "null", "none", "nan"):
                return None
            try:
                return float(val.strip())
            except ValueError:
                return None

        # Parse boolean fields
        def parse_bool(val: Optional[str]) -> bool:
            if not val:
                return False
            return val.strip().lower() in ("true", "1", "t", "yes")

        job_record = {
            "job_id": row.get("job_id", "").strip(),
            "company_name": row.get("company_name", "").strip(),
            "company_domain": row.get("company_domain", "").strip() or None,
            "company_industry": row.get("company_industry", "").strip() or None,
            "ats": row.get("ats", "").strip() or None,
            "ats_token": row.get("ats_token", "").strip() or None,
            "title": row.get("title", "").strip(),
            "department": row.get("department", "").strip() or None,
            "location_raw": row.get("location_raw", "").strip() or None,
            "country": row.get("country", "").strip() or None,
            "city": row.get("city", "").strip() or None,
            "is_remote": parse_bool(row.get("is_remote")),
            "employment_type": row.get("employment_type", "").strip() or "full_time",
            "salary_min": parse_float(row.get("salary_min")),
            "salary_max": parse_float(row.get("salary_max")),
            "salary_currency": row.get("salary_currency", "").strip() or None,
            "salary_period": row.get("salary_period", "").strip() or None,
            "salary_from_text": parse_bool(row.get("salary_from_text")),
            "skills": skills_list,
            "posted_at": row.get("posted_at", "").strip() or None,
            "scraped_at": row.get("scraped_at", "").strip() or None,
            "url": row.get("url", "").strip() or None,
            "ingestion_timestamp": datetime.utcnow().isoformat() + "Z"
        }
        return job_record

    def read_dataset(self) -> Generator[Dict[str, Any], None, None]:
        """
        Generator that streams rows from the CSV file row by row to conserve memory.
        """
        if not os.path.exists(self.csv_path):
            raise FileNotFoundError(f"Dataset CSV not found at '{self.csv_path}'. Please check dataset path.")

        logger.info("Reading job postings from: %s", self.csv_path)
        with open(self.csv_path, mode="r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not self.running:
                    break
                yield self.parse_job_row(row)

    def run(self, loop: bool = False):
        """
        Execute the streaming loop sending events at the designated rate.

        Args:
            loop (bool): If True, continuously repeats the dataset once finished.
        """
        interval = 1.0 / self.rate if self.rate > 0 else 0.0
        logger.info("==================================================")
        logger.info("Starting Kafka Streaming Producer")
        logger.info("Target Topic      : %s", self.topic)
        logger.info("Streaming Rate    : %.2f events/sec (interval: %.4fs)", self.rate, interval)
        logger.info("Record Limit      : %s", f"{self.limit} records" if self.limit else "Unlimited (full file)")
        logger.info("Loop Replay Mode  : %s", loop)
        logger.info("==================================================")

        start_time = time.time()
        last_metric_time = start_time
        records_since_last_metric = 0

        while self.running:
            for record in self.read_dataset():
                if not self.running:
                    break

                target_key = record.get("job_id") or record.get("company_name")
                
                # Asynchronously send message with delivery callbacks
                try:
                    self.producer.send(
                        topic=self.topic,
                        key=target_key,
                        value=record
                    ).add_callback(self._on_send_success).add_errback(self._on_send_error)
                    
                    self.total_sent += 1
                    records_since_last_metric += 1
                except KafkaError as e:
                    self.total_failed += 1
                    logger.error("Error sending message to Kafka: %s", e)

                # Check record limit
                if self.limit and self.total_sent >= self.limit:
                    logger.info("Reached limit of %d records. Stopping stream.", self.limit)
                    self.running = False
                    break

                # Rate limiting sleep
                if interval > 0:
                    time.sleep(interval)

                # Periodic performance logging every 5 seconds
                now = time.time()
                if now - last_metric_time >= 5.0:
                    elapsed = now - last_metric_time
                    current_eps = records_since_last_metric / elapsed
                    overall_eps = self.total_sent / (now - start_time)
                    logger.info(
                        "Streaming Stats -> Sent: %d | Acked: %d | Failed: %d | Velocity: %.1f eps (Overall: %.1f eps)",
                        self.total_sent, self.total_acknowledged, self.total_failed, current_eps, overall_eps
                    )
                    last_metric_time = now
                    records_since_last_metric = 0

            if not loop or not self.running:
                break
            logger.info("Replay mode enabled: Completed file iteration. Replaying dataset...")

        # Final flushing & cleanup
        logger.info("Flushing producer buffer...")
        self.producer.flush(timeout=10)
        self.producer.close()
        
        total_duration = max(time.time() - start_time, 0.001)
        logger.info("==================================================")
        logger.info("Streaming Producer Finished")
        logger.info("Total Sent        : %d", self.total_sent)
        logger.info("Total Acknowledged: %d", self.total_acknowledged)
        logger.info("Total Failed      : %d", self.total_failed)
        logger.info("Total Duration    : %.2f seconds", total_duration)
        logger.info("Average Throughput: %.2f events/sec", self.total_sent / total_duration)
        logger.info("==================================================")


# ------------------------------------------------------------------------------
# CLI Entrypoint
# ------------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Stream tech job postings dataset into Kafka topic with configurable speed."
    )
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to pipeline configuration YAML (default: config/config.yaml)"
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=10.0,
        help="Events to produce per second (default: 10.0 eps)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of records to produce (default: all rows)"
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Loop continuously over the CSV dataset"
    )

    args = parser.parse_args()

    producer = JobPostingsProducer(
        config_path=args.config,
        rate=args.rate,
        limit=args.limit
    )
    producer.run(loop=args.loop)


if __name__ == "__main__":
    main()
