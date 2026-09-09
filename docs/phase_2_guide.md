# Phase 2: Kafka Producer & Streaming Ingestion Guide

## 1. Objective
The objective of Phase 2 is to simulate a production-grade real-time ingestion pipeline by replaying historical job postings from the Hugging Face dataset (394,300+ records) into Apache Kafka. The producer serializes raw tabular records into structured JSON messages, regulates streaming throughput (default: 10 events/sec), and handles message delivery acknowledgements and error recovery.

---

## 2. Architecture & Ingestion Flow

```
+-------------------------------------------------------------+
|               Historical Dataset (CSV)                      |
|         dataset/tech_job_postings.csv (394,300 rows)        |
+-------------------------------------------------------------+
                              |
                              | Streaming Row-by-Row
                              v
+-------------------------------------------------------------+
|                    Python Kafka Producer                    |
|                      (kafka/producer.py)                    |
|  - CSV DictReader with low-memory generator                 |
|  - JSON Schema normalization & type casting                 |
|  - Skill parsing (delimiter split: '|')                     |
|  - Velocity Controller (configurable eps, default: 10)      |
|  - Gzip compression, lingering & asynchronous acks          |
+-------------------------------------------------------------+
                              |
                              | Publish JSON Events
                              v
+-------------------------------------------------------------+
|                     Apache Kafka Broker                     |
|             Topic: 'job_postings' (Partitions: 3)           |
+-------------------------------------------------------------+
            |                                     |
            v                                     v
+------------------------+             +----------------------+
| Spark Structured       |             | CLI Consumer Utility |
| Streaming (Phase 3)    |             | (kafka/consumer.py)  |
+------------------------+             +----------------------+
```

---

## 3. Key Components & Implementation

### A. Producer (`kafka/producer.py`)
- **Memory Efficient Ingestion**: Uses Python generator streaming (`csv.DictReader`) without loading all 394k rows into RAM.
- **Data Normalization**: Converts fields into strong types:
  - `skills`: Pipe-delimited string (`"python|spark|sql"`) -> JSON array (`["python", "spark", "sql"]`)
  - `salary_min` & `salary_max`: Cleaned float casting (or `null`)
  - `is_remote` & `salary_from_text`: Strict boolean parsing
  - `ingestion_timestamp`: Added ISO-8601 UTC timestamp
- **Throughput Regulator**: Dynamically sleeps between messages to enforce precise events-per-second (`--rate 10.0`).
- **Reliability**: Asynchronous delivery callbacks (`on_send_success`, `on_send_error`), batching with `linger_ms=10`, gzip compression, and graceful termination handlers.

### B. Consumer Inspector (`kafka/consumer.py`)
- Lightweight CLI diagnostic tool to verify that Kafka is receiving and broadcasting events accurately.
- Formats incoming JSON payloads into a real-time terminal table or detailed JSON inspection views (`--verbose`).

---

## 4. Commands to Execute

### Step 1: Ensure Kafka is Running (from Phase 1)
```bash
docker-compose -f docker/docker-compose.yml ps
```
*Verify that `kafka` and `zookeeper` containers have status `healthy` or `Up`.*

### Step 2: Run Kafka Producer
```bash
# Basic run: 10 events per second (default)
python kafka/producer.py

# High velocity test: 50 events per second
python kafka/producer.py --rate 50

# Ingest specific sample batch: 500 records
python kafka/producer.py --rate 20 --limit 500

# Infinite replay loop mode:
python kafka/producer.py --rate 10 --loop
```

### Step 3: Inspect Ingested Events using Consumer
```bash
# Preview incoming stream in tabular format
python kafka/consumer.py --limit 20

# Detailed verbose view showing JSON schema fields
python kafka/consumer.py --limit 5 --verbose
```

---

## 5. Expected Output

### Producer Output (`kafka/producer.py`):
```text
2026-09-09 21:15:00 [INFO] [KafkaProducer] Connecting to Kafka broker at: localhost:9092 ...
2026-09-09 21:15:01 [INFO] [KafkaProducer] Successfully connected to Kafka broker! Target Topic: 'job_postings'
2026-09-09 21:15:01 [INFO] [KafkaProducer] ==================================================
2026-09-09 21:15:01 [INFO] [KafkaProducer] Starting Kafka Streaming Producer
2026-09-09 21:15:01 [INFO] [KafkaProducer] Target Topic      : job_postings
2026-09-09 21:15:01 [INFO] [KafkaProducer] Streaming Rate    : 10.00 events/sec (interval: 0.1000s)
2026-09-09 21:15:01 [INFO] [KafkaProducer] Record Limit      : Unlimited (full file)
2026-09-09 21:15:01 [INFO] [KafkaProducer] Loop Replay Mode  : False
2026-09-09 21:15:01 [INFO] [KafkaProducer] ==================================================
2026-09-09 21:15:01 [INFO] [KafkaProducer] Reading job postings from: dataset/tech_job_postings.csv
2026-09-09 21:15:06 [INFO] [KafkaProducer] Streaming Stats -> Sent: 50 | Acked: 50 | Failed: 0 | Velocity: 10.0 eps (Overall: 10.0 eps)
2026-09-09 21:15:11 [INFO] [KafkaProducer] Streaming Stats -> Sent: 100 | Acked: 100 | Failed: 0 | Velocity: 10.0 eps (Overall: 10.0 eps)
```

### Consumer Output (`kafka/consumer.py`):
```text
2026-09-09 21:15:05 [INFO] [KafkaConsumer] Successfully subscribed to topic: 'job_postings'
2026-09-09 21:15:05 [INFO] [KafkaConsumer] Listening for messages on 'job_postings'... Press Ctrl+C to stop.
[00001] 0g                   | Chief of Staff                 | Remote          | Unspecified        | Skills: None
[00002] 0g                   | Product Manager — DeFi Product | Remote          | Unspecified        | Skills: blockchain, product-manag
[00003] 0g                   | Brand & Creative Designer      | Remote          | Unspecified        | Skills: blockchain, communicatio
[00004] 10xteam              | Surgeon - AI Trainer - Freelan | Poland          | EUR 200.0-250.0    | Skills: None
```

---

## 6. Screenshots Placeholder

| Scenario | Screenshot Reference | Description |
| :--- | :--- | :--- |
| **Kafka Producer Streaming** | `[Screenshot: kafka_producer_terminal.png]` | Terminal showing real-time event publishing at 10 eps with acks |
| **Kafka Consumer Live View** | `[Screenshot: kafka_consumer_terminal.png]` | Terminal showing real-time decoded JSON event feed |
| **Kafka Topic Metrics** | `[Screenshot: kafka_broker_metrics.png]` | Topic partition offsets and consumer group lag |

---

## 7. Common Errors & Fixes

| Issue / Error | Root Cause | Solution / Fix |
| :--- | :--- | :--- |
| `NoBrokersAvailable` | Kafka broker container is not yet initialized or port 9092 is unreachable | Run `docker-compose -f docker/docker-compose.yml up -d kafka` and wait 15 seconds for Kafka to register with Zookeeper. |
| `FileNotFoundError: dataset/tech_job_postings.csv` | Dataset file is missing or in wrong path | Verify that `tech_job_postings.csv` is placed inside `dataset/` directory. |
| `TopicAuthorizationException` or `UnknownTopicOrPartitionException` | Kafka topic `job_postings` has not been auto-created | Auto topic creation is enabled in Docker Compose (`KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'`), or create manually: `docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --topic job_postings --partitions 3 --replication-factor 1`. |
| `MemoryError` during CSV parsing | Reading entire 394k CSV into RAM at once | The producer implementation uses streaming `csv.DictReader` generator, consuming < 50MB RAM. Ensure you do not use `.readlines()` or full dataframe reads. |
