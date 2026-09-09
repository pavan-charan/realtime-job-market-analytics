"""
Kafka Consumer Test & Inspection Utility
========================================
Consumes real-time job posting events from the Apache Kafka topic ('job_postings').
Used for validation, throughput verification, and payload inspection.

Features:
- Live deserialization and validation of JSON job records
- Throughput monitoring and formatted table preview of incoming events
- Supports message limits, offset resetting, and topic overrides
"""

import argparse
import json
import logging
import os
import signal
import sys
import time
from typing import Any, Dict, Optional
import yaml
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("KafkaConsumer")


class JobPostingsConsumer:
    """
    Kafka consumer client for topic inspection and pipeline debugging.
    """

    def __init__(
        self,
        config_path: str = "config/config.yaml",
        topic: Optional[str] = None,
        group_id: Optional[str] = None,
        from_beginning: bool = False,
        limit: Optional[int] = None
    ):
        self.config = self._load_config(config_path)
        kafka_cfg = self.config.get("kafka", {})
        
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", kafka_cfg.get("bootstrap_servers", "localhost:9092"))
        self.topic = topic or kafka_cfg.get("topic", "job_postings")
        self.group_id = group_id or kafka_cfg.get("consumer_group", "job_market_inspection_group")
        self.auto_offset_reset = "earliest" if from_beginning else "latest"
        self.limit = limit
        self.running = True
        self.message_count = 0

        self.consumer = self._create_consumer()

        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    @staticmethod
    def _load_config(path: str) -> Dict[str, Any]:
        """Loads YAML configuration file."""
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        return {}

    def _create_consumer(self, retries: int = 10, backoff: float = 3.0) -> KafkaConsumer:
        """Creates KafkaConsumer with connection retry logic."""
        logger.info("Connecting Kafka Consumer to: %s ...", self.bootstrap_servers)
        for attempt in range(1, retries + 1):
            try:
                consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=self.group_id,
                    auto_offset_reset=self.auto_offset_reset,
                    enable_auto_commit=True,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    consumer_timeout_ms=10000  # 10s poll timeout
                )
                logger.info("Successfully subscribed to topic: '%s' (Group: '%s', Offset: %s)",
                            self.topic, self.group_id, self.auto_offset_reset)
                return consumer
            except NoBrokersAvailable as e:
                logger.warning("Attempt %d/%d: Broker not available (%s). Retrying in %.1fs...", attempt, retries, e, backoff)
                time.sleep(backoff)
            except Exception as e:
                logger.error("Failed to connect consumer: %s", e)
                time.sleep(backoff)

        logger.critical("Could not connect consumer after %d attempts. Exiting.", retries)
        sys.exit(1)

    def _handle_shutdown(self, signum, frame):
        """Handle termination signals."""
        logger.info("Shutdown signal received. Closing consumer...")
        self.running = False

    def run(self, verbose: bool = False):
        """
        Poll and consume messages from Kafka topic.
        """
        logger.info("Listening for messages on '%s'... Press Ctrl+C to stop.", self.topic)
        start_time = time.time()
        
        try:
            while self.running:
                # Iterate over incoming messages
                for message in self.consumer:
                    if not self.running:
                        break

                    self.message_count += 1
                    data = message.value

                    title = data.get("title", "N/A")
                    company = data.get("company_name", "N/A")
                    city = data.get("city", "N/A") or "Remote"
                    salary_min = data.get("salary_min")
                    salary_max = data.get("salary_max")
                    curr = data.get("salary_currency", "")
                    skills = ", ".join(data.get("skills", [])) or "None"

                    salary_str = f"{curr} {salary_min}-{salary_max}" if salary_min and salary_max else "Unspecified"

                    if verbose:
                        print("\n" + "=" * 60)
                        print(f"[{self.message_count}] Topic: {message.topic} | Partition: {message.partition} | Offset: {message.offset}")
                        print(f"Job Title : {title}")
                        print(f"Company   : {company} | Location: {city} (Remote: {data.get('is_remote')})")
                        print(f"Salary    : {salary_str}")
                        print(f"Skills    : {skills}")
                        print(f"Ingested  : {data.get('ingestion_timestamp')}")
                    else:
                        print(f"[{self.message_count:05d}] {company:<20} | {title[:30]:<30} | {city:<15} | {salary_str:<18} | Skills: {skills[:30]}")

                    if self.limit and self.message_count >= self.limit:
                        logger.info("Reached target limit of %d consumed messages.", self.limit)
                        self.running = False
                        break

        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()
            duration = max(time.time() - start_time, 0.001)
            logger.info("Consumer stopped. Total records consumed: %d in %.2fs (Avg: %.2f eps)",
                        self.message_count, duration, self.message_count / duration)


def main():
    parser = argparse.ArgumentParser(description="Consume and inspect job postings from Kafka topic.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config.yaml")
    parser.add_argument("--topic", default=None, help="Kafka topic name override")
    parser.add_argument("--group-id", default=None, help="Consumer group ID")
    parser.add_argument("--from-beginning", action="store_true", help="Read from earliest available offset")
    parser.add_argument("--limit", type=int, default=None, help="Max records to consume before exiting")
    parser.add_argument("--verbose", action="store_true", help="Print full JSON message fields")

    args = parser.parse_args()

    consumer = JobPostingsConsumer(
        config_path=args.config,
        topic=args.topic,
        group_id=args.group_id,
        from_beginning=args.from_beginning,
        limit=args.limit
    )
    consumer.run(verbose=args.verbose)


if __name__ == "__main__":
    main()
