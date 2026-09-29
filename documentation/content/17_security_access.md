# 17. Security & Access Control

### Security Controls Implemented
- **Network Isolation:** All backend data services (Kafka, Zookeeper, Hive Metastore, PostgreSQL, HDFS DataNodes) communicate across internal Docker network `bigdata-network`.
- **Database Authentication:** PostgreSQL requires password authentication (`hivepassword`).
- **HiveServer2 Authentication:** Secured via Beeline username/password authentication.
- **Grafana RBAC:** Role-based access control with secure credentials (`admin` / `admin`).
- **Secrets Management:** Environment variables managed through `.env` with templates in `.env.example`.
