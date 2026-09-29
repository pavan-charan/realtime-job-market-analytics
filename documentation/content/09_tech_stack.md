# 9. Technology Stack & Decision Matrix

### 9.1 Core Technology Stack

| Layer | Technology | Version | Purpose & Rationale |
| :--- | :--- | :--- | :--- |
| **Ingestion** | Apache Kafka | 7.5.0 (Confluent) | High-throughput distributed event streaming log. |
| **Coordination** | Apache Zookeeper | 7.5.0 | Kafka cluster coordination and leader election. |
| **Streaming** | PySpark Structured Streaming | 3.5.0 | Fault-tolerant micro-batch processing with watermarking. |
| **Distributed Storage** | Apache Hadoop HDFS | 3.2.1 | Replicated distributed filesystem for Lakehouse storage. |
| **Data Warehouse** | Apache Hive | 3.1.3 | SQL schema over HDFS files with partition pruning. |
| **Metastore Database** | PostgreSQL | 13 | ACID relational metadata store for Hive & Grafana metrics. |
| **Distributed ML** | Spark MLlib | 3.5.0 | Scalable machine learning pipelines across cluster nodes. |
| **Application UI** | Streamlit | 1.30+ | Interactive web interface for salary estimation and analytics. |
| **Observability** | Grafana | 10.2.0 | Real-time monitoring metrics and business intelligence. |

### 9.2 Container Infrastructure
| **zookeeper** | confluentinc/cp-zookeeper:7.5.0 | Containerized Service | 2181:2181 |
| **kafka** | confluentinc/cp-kafka:7.5.0 | Containerized Service | 9092:9092, 29092:29092 |
| **kafka-init** | confluentinc/cp-kafka:7.5.0 | Containerized Service | Internal |
| **namenode** | bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8 | Containerized Service | 9870:9870, 9000:9000 |
| **datanode** | bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8 | Containerized Service | 9864:9864, 9866:9866, 9867:9867 |
| **hive-metastore-db** | postgres:13 | Containerized Service | 5434:5432 |
| **hive-metastore** | apache/hive:3.1.3 | Containerized Service | 9083:9083 |
| **hive-server** | apache/hive:3.1.3 | Containerized Service | 10000:10000, 10002:10002 |
| **spark-master** | apache/spark:3.5.0 | Containerized Service | 7077:7077, 8080:8080 |
| **spark-worker** | apache/spark:3.5.0 | Containerized Service | 8081:8081 |
| **grafana** | grafana/grafana:10.2.0 | Containerized Service | 3001:3000 |
