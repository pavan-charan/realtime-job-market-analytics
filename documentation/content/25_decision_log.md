# 25. Technical Decision Log (Architectural Decision Records)

### ADR 01: Adoption of Medallion Architecture
- **Decision:** Split data pipeline into Bronze (raw), Silver (cleansed), and Gold (Star Schema).
- **Reason:** Guarantees reproducibility; raw data is immutable and can be reprocessed if transformation rules evolve.

### ADR 02: Spark MLlib over Single-Node Scikit-Learn
- **Decision:** Train salary regression models using distributed PySpark MLlib.
- **Reason:** Enables seamless training over millions of rows distributed across Spark cluster nodes.

### ADR 03: Dual-Mode Telemetry & Postgres Mirroring
- **Decision:** Sync Gold aggregates and streaming stats to PostgreSQL for Grafana.
- **Reason:** Enables sub-second dashboard rendering with dynamic parameter filtering without overloading Hive on every refresh.
