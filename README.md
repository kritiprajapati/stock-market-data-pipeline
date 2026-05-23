# Stock Market Data Pipeline

An end-to-end automated data pipeline that ingests live US stock market data, processes it through a Medallion architecture on Azure, detects price anomalies using Machine Learning, and is orchestrated via Apache Airflow and GitHub Actions.

---

## Architecture

![Pipeline Architecture](docs/architecture.png)

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Source | Polygon.io REST API |
| Orchestration | Apache Airflow (Docker) + GitHub Actions |
| Cloud Storage | Azure ADLS Gen2 (Medallion architecture) |
| Processing | Azure Databricks + PySpark + Spark SQL |
| Storage Format | Delta Lake |
| ML / Anomaly Detection | scikit-learn (Isolation Forest) |
| Secret Management | Databricks Secret Scope |
| Version Control | GitHub (with Databricks Git integration) |
| Language | Python |

---

## Pipeline Overview

### Data Flow
Polygon.io API
↓
Airflow / GitHub Actions (scheduled 9AM IST, Tue–Sat)
↓
ADLS Gen2 Bronze (raw JSON)
↓
Databricks Silver (PySpark cleanse + DQ + Delta)
↓
Databricks Gold (Spark SQL metrics + Delta)
↓
Anomaly Detection (Isolation Forest)
↓
ADLS Gen2 Gold (anomaly report + Delta)

### Stocks Tracked
AAPL · GOOGL · MSFT · TSLA · AMZN

---

## Medallion Architecture

### Bronze Layer
- Raw JSON data landed from Polygon.io API
- No transformations — exactly as received
- Partitioned by date: `stock_data/YYYY-MM-DD/raw_stocks.json`

### Silver Layer
- PySpark cleansing and type casting
- 6 Data Quality checks:
  - No null tickers
  - No null trade dates
  - Open price > 0
  - Close price > 0
  - High price >= Low price
  - No duplicate ticker + date combinations
- Written as Delta Lake table partitioned by `trade_date`
- Metadata columns: `ingestion_timestamp`, `source`, `pipeline_version`

### Gold Layer
- PySpark window functions:
  - `LAG()` for daily return % calculation
  - `AVG() OVER` for 7-day moving average
- Spark SQL aggregations:
  - Best performing stock per day (RANK() OVER PARTITION BY)
  - Average metrics per ticker (GROUP BY)
  - Most volatile stocks
  - Bullish/Bearish signals (CASE WHEN vs 7-day MA)
- Written as Delta Lake table

### Anomaly Detection
- Features: close_price, daily_return_pct, price_volatility, price_range_pct, volume
- StandardScaler for feature normalization
- Isolation Forest (contamination=0.1)
- Anomaly score and flag written back to Gold Delta table

---

## Orchestration

### Apache Airflow (local development)
5-task DAG:
1. `fetch_stock_data` — calls Polygon.io API
2. `save_to_bronze` — lands raw data to ADLS
3. `trigger_silver` — calls Databricks REST API
4. `trigger_gold` — calls Databricks REST API
5. `trigger_anomaly` — calls Databricks REST API

### GitHub Actions (production scheduling)
- Runs automatically at 9AM IST, Tuesday to Saturday
- No laptop required
- Secrets stored in GitHub Secrets
- Triggers same pipeline via Python script

---

## Project Structure

stock-pipeline-airflow/
├── dags/
│   └── stock_pipeline_dag.py      # Airflow DAG
├── notebooks/
│   ├── silver_transformation.ipynb
│   ├── gold_transformation.ipynb
│   └── anomaly_detection.ipynb
├── .github/
│   └── workflows/
│       └── stock_pipeline.yml     # GitHub Actions
├── pipeline_runner.py             # GitHub Actions script
├── docker-compose.yaml            # Airflow Docker setup
└── README.md

---

## Setup Guide

### Prerequisites
- Azure account (free tier works)
- Databricks workspace
- Polygon.io free API key
- Docker Desktop
- Python 3.10+

### 1. Clone the repo
```bash
git clone https://github.com/kritiprajapati/stock-market-data-pipeline
cd stock-market-data-pipeline
```

### 2. Set up Azure Storage
- Create ADLS Gen2 storage account
- Create 3 containers: `bronze`, `silver`, `gold`

### 3. Set up Databricks
- Create Azure Databricks workspace
- Create a cluster (13.3 LTS runtime)
- Create secret scope:
```bash
databricks secrets create-scope stock-pipeline
databricks secrets put-secret stock-pipeline azure-storage-key
```

### 4. Set up Airflow
```bash
echo AIRFLOW_UID=50000 > .env
docker compose up airflow-init
docker compose up -d
```
Access at `http://localhost:8080`

### 5. Add Airflow Variables
In Airflow UI (Admin → Variables):
- `polygon_api_key`
- `azure_storage_account`
- `azure_storage_key`
- `databricks_workspace_url`
- `databricks_pat_token`
- `databricks_cluster_id`

### 6. Set up GitHub Actions
Add these secrets in GitHub repo Settings → Secrets:
- `POLYGON_API_KEY`
- `AZURE_STORAGE_ACCOUNT`
- `AZURE_STORAGE_KEY`
- `DATABRICKS_WORKSPACE_URL`
- `DATABRICKS_PAT_TOKEN`
- `DATABRICKS_CLUSTER_ID`

---

## Key Features

- **Dynamic date handling** — notebooks accept execution date from Airflow or fall back to last trading day automatically
- **Secure credential management** — Databricks Secret Scope, no hardcoded secrets
- **Graceful error handling** — weekend/holiday detection, retries on failure
- **Schema evolution ready** — Delta Lake handles schema changes automatically
- **Dual orchestration** — Airflow for development, GitHub Actions for production

---

