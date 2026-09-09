# Kafka Module: Real-Time Event Streaming

This module handles the ingestion and streaming of job posting records from the historical Hugging Face Tech Job Postings dataset into Apache Kafka.

## Architecture

```
[ dataset/tech_job_postings.csv ] (394k+ rows)
               │
               ▼
   [ kafka/producer.py ]
   - Memory-efficient streaming reader
   - JSON transformation & type casting
   - Velocity regulator (default: 10 events/sec)
   - Compression & batching
               │
               ▼
 [ Kafka Broker: localhost:9092 ]
      Topic: 'job_postings'
               │
      ┌────────┴────────┐
      ▼                 ▼
[ spark/streaming.py ]  [ kafka/consumer.py ]
(Bronze Ingestion)      (CLI Stream Inspector)
```

## Files

- **`producer.py`**: Production-ready Kafka Producer that reads CSV data, normalizes records, transforms them into JSON, and publishes them with delivery callbacks and configurable velocity.
- **`consumer.py`**: CLI utility to inspect incoming Kafka messages, monitor ingestion rates, and debug stream payloads.

## Usage Commands

### 1. Start Kafka Producer (Default: 10 events/second)
```bash
python kafka/producer.py
```

### 2. High-Speed Streaming (e.g. 50 events/second)
```bash
python kafka/producer.py --rate 50
```

### 3. Stream a Limited Batch (e.g. First 500 records)
```bash
python kafka/producer.py --rate 20 --limit 500
```

### 4. Continuous Replay Stream
```bash
python kafka/producer.py --rate 15 --loop
```

### 5. Inspect Live Kafka Stream with CLI Consumer
```bash
python kafka/consumer.py --limit 20 --verbose
```
