# Phase 3: Spark Structured Streaming & HDFS Bronze Layer Guide

## 1. Objective
The objective of Phase 3 is to implement the real-time stream processing engine using **Spark Structured Streaming (PySpark)**. It ingests the JSON event stream from Apache Kafka (`job_postings`), performs schema parsing, validation, duplicate filtering via time-based watermarking, extracts skill count metrics, and continuously appends partitioned Bronze Parquet files to Hadoop HDFS (`/data/job_market/bronze`).

---

## 2. Architecture: Bronze Ingestion Flow

```
+-------------------------------------------------------------+
|                     Apache Kafka Broker                     |
|            Topic: 'job_postings' (localhost:9092)           |
+-------------------------------------------------------------+
                              |
                              | Continuous Micro-Batch Ingestion
                              v
+-------------------------------------------------------------+
|             Spark Structured Streaming Engine               |
|                    (spark/streaming.py)                     |
|                                                             |
|  1. Binary Value Deserialization (from_json + StructType)   |
|  2. Schema Validation (drop null job_id / title)            |
|  3. Standardize Timestamps (posted_time, ingestion_time)    |
|  4. Watermarking (10 mins) + Drop Duplicates by job_id     |
|  5. Metadata & Skill Extraction (skills_count, partition)  |
+-------------------------------------------------------------+
                              |
                              | Append Partitioned Parquet
                              v
+-------------------------------------------------------------+
|                     Hadoop HDFS (Bronze)                    |
|             hdfs://localhost:9000/data/job_market/bronze/   |
|                                                             |
|   Directory Partitioning:                                   |
|   └── ingest_year=2026/                                     |
|       └── ingest_month=09/                                  |
|           └── ingest_day=09/                                |
|               └── part-*.parquet                            |
+-------------------------------------------------------------+
```

---

## 3. Key Technical Implementations

### A. Explicit PySpark Schema
Avoids expensive schema inference over unbounded streams by specifying exact types:
```python
StructType([
    StructField("job_id", StringType(), False),
    StructField("company_name", StringType(), True),
    StructField("title", StringType(), True),
    StructField("skills", ArrayType(StringType()), True),
    StructField("salary_min", DoubleType(), True),
    StructField("salary_max", DoubleType(), True),
    ...
])
```

### B. Event-Time Watermarking & Deduplication
To prevent duplicate job records during producer retries or network replays:
```python
df.withWatermark("ingestion_time", "10 minutes").dropDuplicates(["job_id"])
```

### C. Partitioned Parquet Sink with Fault-Tolerant Checkpoints
* **Output Format**: Compressed Parquet partitioned by `ingest_year`, `ingest_month`, `ingest_day`.
* **Checkpointing**: Preserves state and transaction log in HDFS (`/data/job_market/checkpoints/bronze`) ensuring **exactly-once** processing semantics.
* **Micro-Batch Trigger**: Configurable interval (default: 10 seconds).

---

## 4. Commands to Execute

### Step 1: Ensure Kafka and HDFS are Running
```bash
docker-compose -f docker/docker-compose.yml ps
```
*Verify that `kafka`, `zookeeper`, `namenode`, and `datanode` are up.*

### Step 2: Start the Kafka Producer (Background or Separate Terminal)
```bash
python kafka/producer.py --rate 10
```

### Step 3: Start Spark Structured Streaming
```bash
# Run locally with live console monitoring
python spark/streaming.py --console
```

*Or submit via `spark-submit`:*
```bash
spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark/streaming.py --console
```

### Step 4: Verify Bronze Parquet Files in HDFS
```bash
# Inspect directories created in HDFS
docker exec -it namenode hdfs dfs -ls /data/job_market/bronze

# Recursive listing of written Parquet partitions
docker exec -it namenode hdfs dfs -ls -R /data/job_market/bronze
```

---

## 5. Expected Output

### Spark Structured Streaming Console:
```text
2026-09-09 21:20:00 [INFO] [SparkStructuredStreaming] Initializing Spark Session (Master: local[*])...
2026-09-09 21:20:02 [INFO] [SparkStructuredStreaming] Spark Session established successfully. Spark Version: 3.5.0
2026-09-09 21:20:02 [INFO] [SparkStructuredStreaming] Connecting to Kafka topic 'job_postings' at localhost:9092 ...
2026-09-09 21:20:03 [INFO] [SparkStructuredStreaming] ==================================================
2026-09-09 21:20:03 [INFO] [SparkStructuredStreaming] Starting Spark Bronze Streaming Pipeline
2026-09-09 21:20:03 [INFO] [SparkStructuredStreaming] Source Kafka Topic: job_postings
2026-09-09 21:20:03 [INFO] [SparkStructuredStreaming] Bronze Sink Path  : hdfs://localhost:9000/data/job_market/bronze
2026-09-09 21:20:03 [INFO] [SparkStructuredStreaming] Checkpoint Path   : hdfs://localhost:9000/data/job_market/checkpoints/bronze
2026-09-09 21:20:03 [INFO] [SparkStructuredStreaming] Batch Trigger     : 10 seconds
2026-09-09 21:20:03 [INFO] [SparkStructuredStreaming] ==================================================
2026-09-09 21:20:05 [INFO] [SparkStructuredStreaming] Streaming queries active. Awaiting new events...

-------------------------------------------
Batch: 0
-------------------------------------------
+------------------------------------+----------------+------------------------------+--------+---------+----------+----------+------------+--------------------+
|job_id                              |company_name    |title                         |city    |is_remote|salary_min|salary_max|skills_count|ingestion_time      |
+------------------------------------+----------------+------------------------------+--------+---------+----------+----------+------------+--------------------+
|7350655f-4c96-4ceb-9812-e4b42b131559|0g              |Chief of Staff                |NULL    |true     |NULL      |NULL      |0           |2026-09-09 21:20:01 |
|d35c9785-1912-4c23-8d09-dbbe353d4733|0g              |Product Manager — DeFi        |NULL    |true     |NULL      |NULL      |4           |2026-09-09 21:20:02 |
|8a2f757e-4637-45f4-a453-c88ac205420b|0g              |Brand & Creative Designer     |NULL    |true     |NULL      |NULL      |3           |2026-09-09 21:20:03 |
|dc0cf41e-ad58-4ee7-875b-9ebb05294f9b|10xteam         |Surgeon - AI Trainer          |Poland  |true     |200.0     |250.0     |0           |2026-09-09 21:20:04 |
+------------------------------------+----------------+------------------------------+--------+---------+----------+----------+------------+--------------------+
```

### HDFS Parquet Listing Output:
```text
drwxr-xr-x   - root supergroup          0 2026-09-09 21:20 /data/job_market/bronze/ingest_year=2026/ingest_month=09/ingest_day=09
-rw-r--r--   1 root supergroup      18420 2026-09-09 21:20 /data/job_market/bronze/ingest_year=2026/ingest_month=09/ingest_day=09/part-00000-....parquet
```

---

## 6. Screenshots Placeholder

| Scenario | Screenshot Reference | Description |
| :--- | :--- | :--- |
| **Spark Streaming Micro-Batch Execution** | `[Screenshot: spark_streaming_batch_console.png]` | Terminal showing active micro-batches with rows processed |
| **HDFS Web UI Storage** | `[Screenshot: hdfs_web_ui_bronze.png]` | HDFS Web UI (`http://localhost:9870`) browsing Bronze Parquet files |
| **Spark Application UI** | `[Screenshot: spark_ui_streaming_tab.png]` | Spark UI (`http://localhost:8080` or `4040`) Structured Streaming tab showing processing rates & latency |

---

## 7. Common Errors & Fixes

| Issue / Error | Root Cause | Solution / Fix |
| :--- | :--- | :--- |
| `ClassNotFoundException: org.apache.spark.sql.kafka010.KafkaSourceProvider` | Missing Spark-Kafka integration JAR package | Ensure `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0` is provided in SparkSession or `spark-submit`. |
| `ConnectException: Call From ... to localhost:9000 failed on connection exception` | Hadoop NameNode container is down | Run `docker-compose -f docker/docker-compose.yml up -d namenode datanode` and wait until port 9000 is accepting connections. |
| `StreamingQueryException: Cannot write to HDFS path` | HDFS directory permissions or SafeMode active | Leave SafeMode: `docker exec -it namenode hdfs dfsadmin -safemode leave`, then create permissions: `docker exec -it namenode hdfs dfs -mkdir -p /data/job_market && docker exec -it namenode hdfs dfs -chmod -R 777 /data`. |
| Checkpoint mismatch after schema change | Old checkpoint directory contains conflicting transaction log | Delete checkpoint directory in HDFS: `docker exec -it namenode hdfs dfs -rm -r /data/job_market/checkpoints/bronze`. |
