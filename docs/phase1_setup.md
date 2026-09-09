# Phase 1: Environment Setup & Big Data Infrastructure

## 1. Objective
The goal of Phase 1 is to establish a unified, containerized Big Data infrastructure on Docker Compose. This environment will serve as the backbone for streaming ingestion, medallion storage (HDFS), distributed SQL warehousing (Hive), distributed stream & batch processing (Apache Spark), and visualization (Grafana).

---

## 2. Infrastructure Architecture & Service Topology

```
+---------------------------------------------------------------------------------------+
|                                    DOCKER NETWORK (bigdata-net)                       |
|                                                                                       |
|   +-------------------+       +--------------------+       +----------------------+   |
|   | Apache ZooKeeper  |<----->|    Apache Kafka    |<----->|   Kafka-Init Topic   |   |
|   |   (Port: 2181)    |       | (Ports: 9092/29092)|       |   ('job_postings')   |   |
|   +-------------------+       +--------------------+       +----------------------+   |
|                                                                                       |
|   +-------------------+       +--------------------+       +----------------------+   |
|   |  HDFS NameNode    |<----->|   HDFS DataNode    |<----->| PostgreSQL Metastore |   |
|   | (Ports: 9870/9000)|       |   (Port: 9864)     |       |    (Port: 5432)      |   |
|   +-------------------+       +--------------------+       +----------------------+   |
|            ^                                                           ^              |
|            |                                                           |              |
|   +-------------------+       +--------------------+                   |              |
|   |   Hive Metastore  |<----->|    HiveServer2     |<------------------+              |
|   |   (Port: 9083)    |       |(Ports: 10000/10002)|                                  |
|   +-------------------+       +--------------------+                                  |
|                                                                                       |
|   +-------------------+       +--------------------+       +----------------------+   |
|   |   Spark Master    |<----->|    Spark Worker    |       |       Grafana        |   |
|   | (Ports: 7077/8080)|       |    (Port: 8081)    |       |     (Port: 3000)     |   |
|   +-------------------+       +--------------------+       +----------------------+   |
+---------------------------------------------------------------------------------------+
```

### Component Port Mapping Table

| Component | Container Name | Host Port | Internal Port | Purpose / UI |
| :--- | :--- | :--- | :--- | :--- |
| **ZooKeeper** | `zookeeper` | `2181` | `2181` | Kafka cluster metadata & leader election |
| **Kafka Broker** | `kafka` | `9092` | `29092` | Real-time distributed streaming broker |
| **HDFS NameNode** | `namenode` | `9870`, `9000` | `9870`, `9000` | HDFS Web UI (`:9870`) & RPC (`:9000`) |
| **HDFS DataNode** | `datanode` | `9864` | `9864` | HDFS DataNode HTTP Web UI |
| **PostgreSQL** | `hive-metastore-db`| `5432` | `5432` | Hive Metastore relational catalog |
| **Hive Metastore** | `hive-metastore` | `9083` | `9083` | Thrift Metastore API |
| **HiveServer2** | `hive-server` | `10000`, `10002`| `10000`, `10002`| JDBC/ODBC endpoint & HS2 Web UI |
| **Spark Master** | `spark-master` | `8080`, `7077` | `8080`, `7077` | Spark Master Web UI (`:8080`) & RPC |
| **Spark Worker** | `spark-worker` | `8081` | `8081` | Spark Worker Web UI |
| **Grafana** | `grafana` | `3000` | `3000` | Analytics & Monitoring Dashboard |

---

## 3. Configuration & Source Files

All infrastructure files are located in:
* `docker/docker-compose.yml`: Multi-container orchestration definition with health checks and dependencies.
* `config/config.yaml`: Centralized application configuration.
* `.env`: Environment variables and credentials.
* `requirements.txt`: Python 3.11 driver packages.

---

## 4. Commands to Execute

### Step 1: Create Python Virtual Environment & Install Dependencies
```bash
# Navigate to the project root directory
cd job-market-intelligence

# Create Python 3.11 virtual environment
python -m venv venv

# Activate the virtual environment
# On Windows (PowerShell):
.\venv\Scripts\Activate.ps1
# On Linux / macOS:
# source venv/bin/activate

# Upgrade pip and install dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

### Step 2: Start Big Data Cluster via Docker Compose
```bash
cd docker
docker compose up -d
```

### Step 3: Verify All Containers Are Running and Healthy
```bash
docker compose ps
```

### Step 4: Verify Kafka Topic Creation
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Step 5: Verify HDFS Directories & File System
```bash
# Create base HDFS directories for Medallion architecture
docker exec -it namenode hdfs dfs -mkdir -p /data/job_market/bronze
docker exec -it namenode hdfs dfs -mkdir -p /data/job_market/silver
docker exec -it namenode hdfs dfs -mkdir -p /data/job_market/gold
docker exec -it namenode hdfs dfs -mkdir -p /data/job_market/checkpoints
docker exec -it namenode hdfs dfs -mkdir -p /user/hive/warehouse
docker exec -it namenode hdfs dfs -chmod -R 777 /data/job_market
docker exec -it namenode hdfs dfs -chmod -R 777 /user/hive/warehouse

# List created directories
docker exec -it namenode hdfs dfs -ls /data/job_market
```

---

## 5. Expected Output

### A. Docker Status (`docker compose ps`)
```text
NAME                 IMAGE                                           STATUS                    PORTS
zookeeper            confluentinc/cp-zookeeper:7.5.0                 Up (healthy)              0.0.0.0:2181->2181/tcp
kafka                confluentinc/cp-kafka:7.5.0                     Up (healthy)              0.0.0.0:9092->9092/tcp, 0.0.0.0:29092->29092/tcp
kafka-init           confluentinc/cp-kafka:7.5.0                     Exited (0)
namenode             bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8 Up (healthy)              0.0.0.0:9000->9000/tcp, 0.0.0.0:9870->9870/tcp
datanode             bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8 Up (healthy)              0.0.0.0:9864->9864/tcp
hive-metastore-db    postgres:13                                     Up (healthy)              0.0.0.0:5432->5432/tcp
hive-metastore       bde2020/hive-metastore:2.3.2.4-hadoop3.2.1-java8 Up                       0.0.0.0:9083->9083/tcp
hive-server          bde2020/hive-server:2.3.2.4-hadoop3.2.1-java8   Up                        0.0.0.0:10000->10000/tcp, 0.0.0.0:10002->10002/tcp
spark-master         bitnami/spark:3.5.0                             Up (healthy)              0.0.0.0:7077->7077/tcp, 0.0.0.0:8080->8080/tcp
spark-worker         bitnami/spark:3.5.0                             Up                        0.0.0.0:8081->8081/tcp
grafana              grafana/grafana:10.2.0                          Up (healthy)              0.0.0.0:3000->3000/tcp
```

### B. Kafka Topic Listing
```text
job_postings
```

### C. HDFS Directory Listing
```text
Found 4 items
drwxr-xr-x   - root supergroup          0 2026-09-09 13:30 /data/job_market/bronze
drwxr-xr-x   - root supergroup          0 2026-09-09 13:30 /data/job_market/checkpoints
drwxr-xr-x   - root supergroup          0 2026-09-09 13:30 /data/job_market/gold
drwxr-xr-x   - root supergroup          0 2026-09-09 13:30 /data/job_market/silver
```

---

## 6. Screenshots Placeholder

* `[SCREENSHOT 1.1]`: Docker Desktop / terminal showing all 11 containers running and healthy.
* `[SCREENSHOT 1.2]`: Apache Hadoop HDFS NameNode Web UI (`http://localhost:9870`).
* `[SCREENSHOT 1.3]`: Apache Spark Master Web UI (`http://localhost:8080`) with 1 registered worker (2 Cores, 2GB RAM).
* `[SCREENSHOT 1.4]`: Grafana Login / Welcome Screen (`http://localhost:3000`).

---

## 7. Common Errors & Fixes

### Error 1: Port Conflict (e.g. `Bind for 0.0.0.0:5432 failed: port is already allocated`)
* **Cause**: A local instance of PostgreSQL, Kafka, or Grafana is already running on the host.
* **Fix**: Stop the host service or edit `.env` / `docker/docker-compose.yml` to remap the external port (e.g., `5433:5432`).

### Error 2: HDFS SafeMode Active (`Cannot create directory ... Name node is in safe mode`)
* **Cause**: NameNode is starting up and inspecting block reports from DataNodes.
* **Fix**: Wait 30 seconds, or manually leave safe mode using:
  ```bash
  docker exec -it namenode hdfs dfsadmin -safemode leave
  ```

### Error 3: Kafka Connection Refused (`NoBrokersAvailable`)
* **Cause**: Kafka broker is still initializing or ZooKeeper was not ready.
* **Fix**: Verify Kafka is healthy with `docker logs kafka`. External Python scripts running on host use `localhost:9092`; internal Docker containers use `kafka:29092`.

### Error 4: Out of Memory on Docker Engine
* **Cause**: Total allocated memory in Docker Desktop is lower than the sum of containers (~6GB recommended).
* **Fix**: In Docker Desktop Settings -> Resources -> Set Memory to at least 8 GB and CPUs to 4.
