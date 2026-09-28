# Real-Time Fraud Prevention & Intelligent Analytics Engine

An enterprise-grade, distributed streaming intelligence pipeline designed to score financial transactions, detect fraudulent checkout events in real time (< 50ms SLA), and route dual-sink analytical telemetry across cloud data warehouses and message brokers.

[![Python 3.8+](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/)
[![Apache Spark 3.5.0](https://img.shields.io/badge/Apache%20Spark-3.5.0-orange.svg)](https://spark.apache.org/)
[![Apache Kafka 4.3.1](https://img.shields.io/badge/Apache%20Kafka-4.3.1-black.svg)](https://kafka.apache.org/)
[![Apache NiFi 2.12.0](https://img.shields.io/badge/Apache%20NiFi-2.12.0-red.svg)](https://nifi.apache.org/)
[![MLflow 2.10.0](https://img.shields.io/badge/MLflow-2.10.0-0194E2.svg)](https://mlflow.org/)
[![LightGBM](https://img.shields.io/badge/Model-LightGBM-brightgreen.svg)](https://lightgbm.readthedocs.io/)
[![Prometheus](https://img.shields.io/badge/Monitoring-Prometheus-E6522C.svg)](https://prometheus.io/)
[![Grafana](https://img.shields.io/badge/Visualization-Grafana-F46800.svg)](https://grafana.com/)

---

## Architecture Overview

```text
[ Synthetic Generator (Faker) ] 
              │
              ▼ (HTTP POST JSON)
[ Apache NiFi 2.12 Ingest (:8082 / :8443) ]
              │
              ▼ (Publish)
[ Apache Kafka (Topic: transactions.incoming) ]
              │
              ▼ (Micro-Batch Stream)
[ Apache Spark 3.5.0 Structured Streaming ]
       ├── MLflow Model Registry (Production LightGBM Booster)
       ├── Vectorized PyArrow / Pandas UDF (<5ms scoring)
       ├── Optimal Decision Threshold (tau* = 0.10)
       ├── Embedded Prometheus Server (:8000 /metrics)
       │
       ├──► [ Kafka Topic: transactions.alerts ] ──► (Downstream Incident / 2FA)
       └──► [ Google Cloud BigQuery ]            ──► (MLOps Archive & Retraining)
```

> [!TIP]
> For complete technical diagrams, lifecycle sequences, feature pipelines, and ER topologies, see [**`DIAGRAMS.md`**](file:///c:/Users/valal/Documents/Development/GitHub/Real-Time-Fraud-Prevention-Intelligent-Analytics-Engine/DIAGRAMS.md).

---

## Service Directory & Port Map

All infrastructure services run containerized via `docker-compose.yml`:

| Service | Container Name | Internal Port | Host Port | Web UI / Access URL | Credentials |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Apache NiFi** | `nifi-ingest` | `8443`, `8082` | `8443`, `8082` | `https://localhost:8443/nifi`<br/>`http://localhost:8082/contentListener` | `admin`<br/>`SuperSecretPassword123` |
| **Apache Kafka** | `kafka-broker` | `9092`, `9093` | `9092` | `kafka-broker:9092` (KRaft Mode) | *None* |
| **Kafka UI** | `kafka-ui` | `8080` | `8085` | `http://localhost:8085` | *None* |
| **Spark Streaming** | `spark-streaming` | `4040`, `8000` | `4040`, `8000` | `http://localhost:4040` (Spark UI)<br/>`http://localhost:8000/metrics` (Prometheus) | *None* |
| **MLflow Server** | `mlflow-server` | `5000` | `5000` | `http://localhost:5000` | *None* |
| **PostgreSQL (MLflow DB)** | `mlflow-db` | `5432` | *Internal* | `mlflow-db:5432` | `mlflow` / `password` |
| **Jupyter Sandbox** | `jupyter-sandbox` | `8888` | `8888` | `http://localhost:8888` | Token in logs |
| **Prometheus** | `prometheus` | `9090` | `9090` | `http://localhost:9090` | *None* |
| **Grafana** | `grafana` | `3000` | `3000` | `http://localhost:3000` | `admin` / `admin` |
| **Kafka Exporter** | `kafka-exporter`| `9308` | `9308` | `http://localhost:9308/metrics` | *None* |

---

## Machine Learning & Feature Engineering Architecture

### 1. Eliminating the Non-Reproducible Feature Anti-Pattern
The IEEE-CIS Fraud Detection dataset contains 434 raw columns, including **339 proprietary Vesta features ($V_1$ to $V_{339}$)** and dozens of anonymized identity masks ($id_{01}$ to $id_{38}$). 

Training models on raw $V$-features for streaming production is an anti-pattern:
* $V$-features are proprietary calculations with unknown definitions, uncomputable within a 50ms streaming window.
* Feeding missing/dummy defaults into the model at runtime severely degrades inference accuracy.

**Engineering Solution:**
* Dropped all 339 $V$-columns and obscured $id$ masks offline across all **590,540 rows**.
* Trained exclusively on a **30-feature reproducible subset**: raw monetary primitives, device signals, card network metadata, temporal cyclical transforms, and domain interaction keys.

### 2. Feature Engineering Pipeline ([`ml/features/Features.ipynb`](file:///c:/Users/valal/Documents/Development/GitHub/Real-Time-Fraud-Prevention-Intelligent-Analytics-Engine/ml/features/Features.ipynb))
* **Cyclical Temporal Encoding**: Converts raw timestamps into continuous periodic signals:
  $$\text{Hour\_sin} = \sin\left(\frac{2\pi \cdot \text{Hour}}{24}\right), \quad \text{Hour\_cos} = \cos\left(\frac{2\pi \cdot \text{Hour}}{24}\right)$$
* **Monetary Transforms**: Log-transformed transaction amount $\ln(1 + \text{amount})$ and cents extraction $(\text{Amt} - \lfloor\text{Amt}\rfloor)$ to detect round-dollar fraud spikes.
* **Domain Interaction**: Strict recipient-sender domain comparison (`email_match`).
* **Entity Interaction**: High-cardinality composite key `card1_addr1` combining card BIN and billing region.

### 3. Training & Registry ([`ml/training/Training.ipynb`](file:///c:/Users/valal/Documents/Development/GitHub/Real-Time-Fraud-Prevention-Intelligent-Analytics-Engine/ml/training/Training.ipynb))
* LightGBM Classifier trained with early stopping on 472k training samples.
* Automatic experiment tracking with parameters, ROC-AUC metrics, and LightGBM model artifact logged to MLflow (`http://mlflow:5000`).
* Registered model `FraudDetectionModel` promoted to the `"Production"` registry stage.

### 4. Financial Cost-Matrix Optimization ([`ml/evaluation/Evaluation.ipynb`](file:///c:/Users/valal/Documents/Development/GitHub/Real-Time-Fraud-Prevention-Intelligent-Analytics-Engine/ml/evaluation/Evaluation.ipynb))
Standard classification models apply an arbitrary 0.50 probability threshold. In financial fraud detection, the costs of errors are asymmetric:
* **False Negative (Missed Fraud):** Direct chargeback loss equal to transaction amount plus fees ($\approx \$150$).
* **False Positive (False Alert):** Customer friction and verification operational cost ($\approx \$10$).

By optimizing the cost matrix over holdout validation data, the optimal financial decision threshold was identified as **$\tau^* = 0.10$**, maximizing net financial savings over the default 0.50 threshold while maintaining sub-10ms inference latency.

---

## Real-Time Streaming & Inference Engine

### 1. Vectorized PyArrow / Pandas UDF
Implemented in [`spark/jobs/streaming_inference.py`](file:///c:/Users/valal/Documents/Development/GitHub/Real-Time-Fraud-Prevention-Intelligent-Analytics-Engine/spark/jobs/streaming_inference.py):
* Uses PySpark `@pandas_udf(DoubleType())` with Apache Arrow acceleration.
* Lazily instantiates the production LightGBM Booster inside worker processes via a singleton pattern.
* **Categorical Alignment**: Dynamically aligns string inputs to `pd.Categorical` against `booster.pandas_categorical`. Unseen runtime categories are gracefully cast to `NaN`, which LightGBM handles natively without failing.
* Derives cyclical, monetary, and interaction features in memory under **5ms**.

### 2. Dual-Sink Micro-Batch Routing
Each micro-batch evaluated in `route_micro_batch(batch_df, batch_id)` is dispatched through two parallel pipelines:
1. **Google Cloud BigQuery**: Direct append of complete transaction records (both legitimate and fraudulent) to `fraud_analytics.transactions_log` for continuous model audit and offline retraining.
2. **Kafka Alerts Topic**: High-risk transactions ($P(\text{isFraud}) \ge 0.10$) are immediately filtered and published to `transactions.alerts` for downstream incident response, automated user SMS challenge, or account suspension.

---

## Observability & Telemetry

The stack provides full operational visibility across infrastructure and model inference:

* **Prometheus Server (`:9090`)**: Scrapes metrics every 5 seconds from:
  * `spark-streaming:8000/metrics`: Transaction count, fraud alert volume, batch processing duration, and probability score distributions.
  * `kafka-exporter:9308/metrics`: Broker partition offsets and consumer group lag.
* **Pre-Configured Grafana Dashboard (`:3000`)**:
  * **Ingestion Throughput**: Live transactions per second (`rate(transactions_processed_total[1m])`).
  * **Consumer Group Lag**: Real-time Kafka partition lag to identify stream starvation or backpressure.
  * **SLA Latency Tracking**: Spark micro-batch processing duration tracked against the 50ms SLA.
  * **Live Fraud Alert Ratio**: Real-time ratio of blocked transactions to total volume.
  * **Model Drift Monitoring**: Real-time score distribution tracked via rolling window counts `sum(increase(fraud_probability_distribution_bucket[1m])) by (le)` to avoid cumulative counter accumulation traps and detect true distribution shifts.

---

## Quickstart Guide

### 1. Prerequisites
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) (with Docker Compose v2)
* Python 3.8+ (for running the synthetic transaction simulator)
* 8 GB+ RAM allocated to Docker

### 2. Clone the Repository
```bash
git clone https://github.com/NightKing-V/Real-Time-Fraud-Prevention-Intelligent-Analytics-Engine.git
cd Real-Time-Fraud-Prevention-Intelligent-Analytics-Engine
```

### 3. Launch the Infrastructure Stack
Build and launch all services in detached mode:
```bash
docker compose up -d --build
```

Verify that all 10 containers are healthy:
```bash
docker compose ps
```

### 4. Set Up the Python Simulator Environment
```bash
cd generator
python -m venv .venv
# On Windows:
.\.venv\Scripts\activate
# On Linux/macOS:
# source .venv/bin/activate

pip install -r requirements.txt
```

### 5. Fire Simulated Live Transactions
Run the transaction generator to emit synthetic checkout events matching the offline schema to Apache NiFi:
```bash
python transaction_generator.py
```

### 6. Monitor Live Processing
* **Grafana Dashboards**: Open [http://localhost:3000](http://localhost:3000) (Login: `admin` / `admin`). Navigate to Dashboards $\to$ **Real-Time Fraud Prevention & Intelligent Analytics**.
* **Spark Structured Streaming Logs**:
  ```bash
  docker logs -f spark-streaming
  ```
* **Kafka UI**: Open [http://localhost:8085](http://localhost:8085) to view incoming messages in `transactions.incoming` and blocked alerts in `transactions.alerts`.
* **MLflow UI**: Open [http://localhost:5000](http://localhost:5000) to inspect experiment runs, feature importances, and registered models.

---

## Repository Structure

```text
.
├── DIAGRAMS.md                     # Comprehensive Mermaid system & sequence diagrams
├── README.md                       # Main project documentation
├── docker-compose.yml              # Multi-container orchestration definition
│
├── generator/                      # Synthetic data generation layer
│   ├── transaction_generator.py    # Faker simulator emitting IEEE-CIS primitives
│   └── requirements.txt            # Faker and requests dependencies
│
├── ml/                             # Offline machine learning & MLOps platform
│   ├── data/ieee-fraud-detection/  # IEEE-CIS dataset & train_features.parquet
│   ├── features/
│   │   └── Features.ipynb          # EDA, memory downcasting, feature pruning
│   ├── training/
│   │   └── Training.ipynb          # LightGBM training & MLflow model registration
│   └── evaluation/
│       └── Evaluation.ipynb        # Cost-matrix optimization & latency benchmark
│
├── mlflow/                         # MLflow server docker configuration
│   ├── Dockerfile
│   └── requirements.txt
│
├── spark/                          # Real-time distributed streaming engine
│   ├── Dockerfile                  # Custom Spark image with MLflow & PyArrow
│   ├── requirements.txt            # PySpark, LightGBM, MLflow, Prometheus dependencies
│   ├── config/                     # Google Cloud service account credentials
│   └── jobs/
│       └── streaming_inference.py  # Production streaming inference & dual-sink router
│
└── monitoring/                     # Observability & telemetry configurations
    ├── prometheus.yml              # Prometheus scrape targets & intervals
    └── grafana/
        └── provisioning/
            ├── datasources/        # Automated Prometheus datasource config
            └── dashboards/         # Automated pre-built fraud dashboard JSON
```

---

## Configuration & GCP BigQuery Credentials

To enable Google Cloud BigQuery streaming ingestion:
1. Place your GCP Service Account JSON key inside `./spark/config/credentials.json`.
2. Ensure the service account has the **BigQuery Data Editor** and **BigQuery Job User** IAM roles.
3. Set your GCP project ID in `docker-compose.yml` under the `spark` service environment:
   ```yaml
   environment:
     - GOOGLE_APPLICATION_CREDENTIALS=/app/config/credentials.json
     - GCP_PROJECT=your-gcp-project-id
   ```
4. Create the target dataset and table in BigQuery:
   ```sql
   CREATE DATASET IF NOT EXISTS fraud_analytics;
   ```

---

## License & Credits

* **Dataset**: [IEEE-CIS Fraud Detection](https://www.kaggle.com/c/ieee-fraud-detection) hosted by Vesta Corporation.
* Developed as an end-to-end production architecture for real-time streaming intelligence and MLOps.
