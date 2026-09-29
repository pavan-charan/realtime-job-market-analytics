# 12. API & Service Interface Reference

### Service Ports & Protocols

| Service Name | Protocol | Port | Authentication | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| **Kafka Broker** | TCP / Kafka | `9092` | PLAINTEXT | Event streaming broker |
| **HDFS NameNode Web** | HTTP | `9870` | None | HDFS cluster file browser & metrics |
| **HDFS IPC** | TCP / HDFS | `9000` | None | Hadoop IPC RPC filesystem endpoint |
| **HiveServer2 Beeline** | JDBC / Thrift | `10000` | `hive` / `hivepassword` | SQL query execution engine |
| **Hive Metastore** | Thrift | `9083` | None | Relational schema metastore service |
| **Spark Master UI** | HTTP | `8080` | None | Spark cluster status & active jobs |
| **Spark Master RPC** | TCP | `7077` | None | Distributed driver submission |
| **PostgreSQL DB** | TCP / PGSQL | `5434` (mapped) | `hive` / `hivepassword` | Metastore & Grafana analytics database |
| **Grafana UI** | HTTP | `3001` | `admin` / `admin` | Real-time monitoring & BI dashboards |
| **Streamlit App** | HTTP | `8501` | None | Interactive prediction web app |
