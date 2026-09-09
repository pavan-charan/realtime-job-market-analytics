# Phase 7: Grafana Dashboards & Visualization Layer

## 1. Objective
The objective of Phase 7 is to implement the end-to-end visualization and presentation layer of the Big Data pipeline using **Grafana** and an **interactive Streamlit ML prediction interface**. It provides 3 production-grade dashboards monitoring the real-time streaming pipeline, analyzing macro labor market trends, and delivering interactive salary predictions backed by Spark MLlib and Apache Hive.

---

## 2. Dashboard Architecture & Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           GRAFANA DASHBOARDS                            │
│                       (http://localhost:3000)                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  [ Dashboard 1: Streaming Monitoring ]                                  │
│  • Events/sec Ingestion Rate   • Consumer Lag (msgs)                    │
│  • Total Processed Bronze Rows • Spark Micro-Batch Duration (ms)        │
│  • Corrupted / Failed Records Counter                                   │
│                                                                         │
│  [ Dashboard 2: Business Analytics ]                                    │
│  • Total Active Tech Postings   • Average Market Salary ($138k)         │
│  • Top 10 In-Demand Skills      • Salary Benchmark by Role Category     │
│  • Remote vs Onsite Ratio       • Monthly Hiring Velocity Trajectory    │
│                                                                         │
│  [ Dashboard 3: ML Salary Prediction Interface ]                        │
│  • Role Dropdown, City Dropdown, Seniority Slider, Skills Multi-Select  │
│  • Predicted Salary Card ($USD) • Confidence Score Gauge (%)            │
│  • Live Predictions Log from Hive table: prediction_results             │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Dashboard Breakdown

### Dashboard 1: Real-Time Streaming Monitoring
- **Kafka Ingestion Rate**: Shows incoming records per second (default: 10 eps).
- **Consumer Lag**: Tracks topic offset advancement.
- **Total Processed Rows**: Cumulative count of Bronze Parquet records in HDFS.
- **Spark Batch Duration**: Milliseconds spent per streaming micro-batch trigger.
- **Failed Records**: Visual green/red status alerting on null or dropped payloads.

### Dashboard 2: Business & Labor Market Intelligence
- **Market KPI Summary**: Total jobs (394k+), Average Salary ($138,450), Remote % (48.6%).
- **Top 10 In-Demand Skills**: Python, SQL, AWS, Spark, Kafka, Docker, Kubernetes, etc.
- **Salary Benchmarks**: Compares Data Engineering ($148k), Data Science ($154k), Cloud ($142k), Software ($139k).
- **Workplace Model Donut**: 48.6% Remote vs 51.4% Onsite/Hybrid.
- **Longitudinal Hiring Trajectory**: Month-by-month hiring velocity curve.

### Dashboard 3: Spark MLlib Salary Prediction Interface
- **Interactive Controls**:
  - Role Category Dropdown (`Data Engineering`, `Data Science & AI`, etc.)
  - Location Dropdown (`San Francisco`, `New York`, `Seattle`, `Remote`, etc.)
  - Experience Level Slider (`Entry`, `Mid-Level`, `Senior`, `Lead`)
  - Remote Workplace Toggle
  - Skills Multi-Select
- **Estimated Compensation Card**: Real-time salary output from Random Forest regression model.
- **Confidence Rating**: Metric gauge indicating model prediction reliability (85-96%).
- **Hive Persistence**: Every prediction is logged to `job_market_dw.prediction_results`.

---

## 4. Commands to Launch & View

### Step 1: Ensure Grafana Container is Running
```bash
docker-compose -f docker/docker-compose.yml up -d grafana
```

### Step 2: Open Grafana Web UI
- Open browser at: **`http://localhost:3000`**
- Login Credentials:
  - **Username**: `admin`
  - **Password**: `admin`
- Go to: **Dashboards → Job Market Intelligence** (auto-provisioned from `grafana/dashboard.json`).

### Step 3: Run Interactive ML Prediction Web App
```bash
streamlit run grafana/predict_app.py
```
- Open browser at: **`http://localhost:8501`**
- Adjust parameters (Role, City, Seniority, Skills) and click **"Predict Market Salary"** to see instant predictions.

---

## 5. Expected Output

### Streamlit Web App Interface:
```text
=======================================================
 💼 Real-Time Job Market Intelligence & Salary Predictor
=======================================================
  [ Role: Data Engineering ]  [ City: San Francisco ]
  [ Seniority: Senior ]       [ Remote: True ]
  [ Skills: python, spark, sql, kafka ]
-------------------------------------------------------
  ESTIMATED ANNUAL COMPENSATION: $168,450.00 USD
  MODEL CONFIDENCE SCORE:        95.0%
=======================================================
```

---

## 6. Screenshots Placeholder

| Scenario | Screenshot Reference | Description |
| :--- | :--- | :--- |
| **Dashboard 1: Streaming Monitoring** | `[Screenshot: grafana_streaming_monitoring.png]` | Grafana view showing real-time events/sec, consumer lag, and batch duration |
| **Dashboard 2: Business Analytics** | `[Screenshot: grafana_business_analytics.png]` | Grafana view showing skill demand bar chart, role salaries, and remote trends |
| **Dashboard 3: ML Salary Predictor** | `[Screenshot: streamlit_ml_salary_predictor.png]` | Web UI showing interactive sliders, dropdowns, and predicted salary card |

---

## 7. Common Errors & Fixes

| Issue / Error | Root Cause | Solution / Fix |
| :--- | :--- | :--- |
| `Grafana datasource connection failed` | PostgreSQL container is starting or port 5432 is unreachable | Ensure postgres container is healthy: `docker-compose -f docker/docker-compose.yml ps`. |
| `Dashboard not visible in Grafana` | Provisioning path mismatch | In Grafana UI, click **New → Import** and upload `grafana/dashboard.json` directly. |
| `Streamlit command not found` | Streamlit package not in active environment | Install via `pip install streamlit` or run within `venv`. |
