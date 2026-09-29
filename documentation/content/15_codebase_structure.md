# 15. Codebase Structure & Component Inventory

### Repository Layout
```text
job-market-intelligence/
├── config/
│   └── config.yaml               # Central pipeline, Spark, Kafka, and model configs
├── dataset/
│   ├── tech_job_postings.csv     # Raw dataset source
│   └── sample_job_postings.csv   # Lightweight test sample
├── docker/
│   └── docker-compose.yml        # 10 container cluster orchestration
├── docs/
│   ├── phase1_setup.md           # Setup verification guide
│   ├── phase_7_guide.md          # Grafana monitoring documentation
│   └── model_metrics.json        # Serialized ML model evaluation metrics
├── grafana/
│   └── dashboard.json            # 3-in-1 live monitoring and BI dashboard
├── hadoop_home/                  # Windows native winutils and Hadoop binaries
│   └── bin/ (hadoop.dll, winutils.exe)
├── hive/
│   ├── schema.sql                # HiveQL Star Schema DDL
│   └── queries.sql               # 20 Enterprise BI Queries
├── kafka/
│   └── producer.py               # Streaming producer with live telemetry
├── scripts/
│   └── sync_postgres_tables.py   # High-speed PostgreSQL analytics synchronization
├── spark/
│   ├── streaming.py              # Structured Streaming Kafka-to-Bronze consumer
│   ├── etl.py                    # Medallion ETL (Bronze -> Silver -> Gold)
│   ├── feature_engineering.py    # Spark MLlib Feature Pipeline
│   ├── train_model.py            # Distributed ML training script
│   └── predict.py                # Real-time inference utility
├── streamlit_app.py              # Interactive Web Application
├── documentation/                # Modular documentation and generated PDF
├── docs.py                       # Unified CLI documentation tool
└── requirements.txt              # Python virtual environment dependencies
```

**Scanned Source Files:** 74  
**Total Source Lines of Code:** 403156
