# Grafana Dashboards & Visualization Module

This module contains the Grafana dashboard configurations, datasource provisioning, and interactive ML prediction web application.

## Dashboards Overview

### Dashboard 1: Streaming Monitoring
- **Events/sec Ingestion Rate**: Live Kafka throughput meter.
- **Consumer Lag**: Kafka partition offsets vs consumer group lag.
- **Processed Rows**: Cumulative Bronze record count.
- **Batch Duration**: Spark Structured Streaming micro-batch processing latency (ms).
- **Failed Records**: Error/corrupted message counter.

### Dashboard 2: Business & Market Analytics
- **Total Jobs & Companies**: Macro labor market KPIs.
- **Top 10 In-Demand Skills**: Python, SQL, AWS, Spark, Kafka, Docker, Kubernetes.
- **Hiring by City**: Compensation and volume across major tech hubs.
- **Salary Distribution**: Boxplot and benchmarks by role category.
- **Remote vs. Onsite**: Donut chart and remote salary premium comparison.
- **Monthly Hiring Velocity**: Time-series hiring trends.

### Dashboard 3: ML Salary Prediction Interface
- **Job Role Dropdown**, **City Dropdown**, **Experience Slider**, **Skills Multi-Select**.
- **Predicted Annual Salary Card** & **Model Confidence Gauge**.
- **Interactive Web App**: Available via Streamlit at `http://localhost:8501`.
- **Hive Integration**: Reads and writes results to `job_market_dw.prediction_results`.

---

## How to Access Dashboards

1. **Grafana Web UI**:
   - URL: `http://localhost:3000`
   - Username: `admin`
   - Password: `admin`
   - Navigate to: **Dashboards → Job Market Intelligence**

2. **Run Interactive ML Predictor Web App**:
   ```bash
   streamlit run grafana/predict_app.py
   ```
   Open `http://localhost:8501` in your browser.
