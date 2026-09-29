# 18. Offline, Resilience & Fault-Tolerance Handling

### Fault Tolerance Mechanisms
1. **Kafka Replicated Partitions:** Durable disk-backed commit log prevents data loss during upstream network disconnects.
2. **Spark Structured Streaming Checkpointing:** Write-ahead logs and state stores ensure exact-once processing upon worker restart.
3. **Hadoop HDFS Block Replication:** Standard block replication distributes data across DataNodes for hardware resilience.
4. **Graceful Producer Shutdown:** Intercepts `SIGINT` / `SIGTERM` signals, flushes buffers, and commits Kafka offsets cleanly.
