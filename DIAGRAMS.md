# System Architecture & Pipeline Diagrams

This document contains architectural diagrams and technical flowcharts for the **Real-Time Fraud Prevention & Intelligent Analytics Engine**.

---

## 1. End-to-End System Architecture

```mermaid
flowchart TD
    subgraph INGESTION["1. Data Ingestion & Generation"]
        GEN["Synthetic Transaction Generator<br/>(Faker + IEEE-CIS Primitives)"]
        NIFI["Apache NiFi 2.12<br/>(:8082 HTTP Listen / :8443 UI)"]
        GEN -->|"HTTP POST JSON<br/>(10-100 tx/sec)"| NIFI
    end

    subgraph MESSAGING["2. Streaming Event Backbone"]
        KAFKA["Apache Kafka 4.3 (KRaft)<br/>(:9092 PLAINTEXT)"]
        TOPIC_IN["Topic: transactions.incoming<br/>(Raw Event Stream)"]
        KAFKA_UI["Kafka UI Dashboard<br/>(:8085)"]
        
        NIFI -->|"Publish Standardized JSON"| TOPIC_IN
        TOPIC_IN --- KAFKA
        KAFKA --- KAFKA_UI
    end

    subgraph MLOPS["3. Machine Learning Platform"]
        DATA["IEEE-CIS Dataset<br/>(590k records x 434 cols)"]
        PRUNE["Feature Pruning Engine<br/>(Drops 339 V-cols & 38 ID masks)"]
        TRAIN["LightGBM Classifier<br/>(Training.ipynb)"]
        EVAL["Model Validation & Cost Matrix<br/>(Evaluation.ipynb: tau* = 0.10)"]
        MLFLOW["MLflow Tracking & Model Registry<br/>(:5000 Server / Postgres DB)"]
        
        DATA --> PRUNE
        PRUNE -->|"30 Reproducible Features"| TRAIN
        TRAIN -->|"Register Artifact"| MLFLOW
        EVAL -->|"Promote to 'Production'"| MLFLOW
    end

    subgraph STREAMING["4. Real-Time Distributed Inference Engine"]
        SPARK["Apache Spark 3.5.0 Structured Streaming<br/>(:4040 UI)"]
        UDF["Vectorized Arrow Pandas UDF<br/>(Categorical Alignment + <5ms Scoring)"]
        DECISION{"Decision Threshold<br/>(P_fraud >= 0.10)"}
        
        TOPIC_IN -->|"Micro-Batch Stream"| SPARK
        MLFLOW -.->|"Load Production Booster"| UDF
        SPARK --> UDF
        UDF --> DECISION
    end

    subgraph SINKS["5. Dual-Sink Routing"]
        TOPIC_OUT["Topic: transactions.alerts<br/>(Blocked Event Payloads)"]
        BQ["Google Cloud BigQuery<br/>(fraud_analytics.transactions_log)"]
        
        DECISION -->|"is_fraud_alert == 1<br/>(BLOCK_AND_ALERT)"| TOPIC_OUT
        DECISION -->|"All Transactions<br/>(MLOps Archive)"| BQ
    end

    subgraph OBSERVABILITY["6. Observability & Telemetry"]
        K_EXP["Kafka Exporter<br/>(:9308)"]
        S_PROM["Spark Embedded HTTP Server<br/>(:8000 /metrics)"]
        PROM["Prometheus TSDB<br/>(:9090 Scraping @ 5s)"]
        GRAF["Grafana Dashboard<br/>(:3000 Auto-Provisioned)"]
        
        KAFKA --> K_EXP
        SPARK --> S_PROM
        K_EXP -->|"Scrape Lag"| PROM
        S_PROM -->|"Scrape Throughput, Latency & Drift"| PROM
        PROM -->|"Visualize"| GRAF
    end

    classDef primary fill:#2563eb,stroke:#1d4ed8,color:#ffffff;
    classDef secondary fill:#059669,stroke:#047857,color:#ffffff;
    classDef alert fill:#dc2626,stroke:#b91c1c,color:#ffffff;
    classDef storage fill:#d97706,stroke:#b45309,color:#ffffff;
    
    class GEN,NIFI,SPARK primary;
    class UDF,MLFLOW,TRAIN secondary;
    class TOPIC_OUT alert;
    class KAFKA,BQ,PROM,GRAF storage;
```

---

## 2. Real-Time Transaction Lifecycle Sequence

```mermaid
sequenceDiagram
    autonumber
    participant Gen as Transaction Generator
    participant NiFi as Apache NiFi
    participant KIn as Kafka (transactions.incoming)
    participant Spark as Spark Structured Streaming
    participant ML as MLflow (Production Booster)
    participant BQ as GCP BigQuery
    participant KAlert as Kafka (transactions.alerts)
    participant Prom as Prometheus / Grafana

    Note over Gen,NiFi: Ingestion Phase (<10ms)
    Gen->>NiFi: HTTP POST /contentListener (JSON Event)
    NiFi->>KIn: Produce to 'transactions.incoming'
    
    Note over Spark,ML: Real-Time Scoring Phase (<15ms)
    Spark->>KIn: Poll latest micro-batch
    Spark->>Spark: Parse JSON Schema & Extract Primitives
    Spark->>ML: Lazy Load / Cache LightGBM Booster
    Spark->>Spark: Vectorized Feature Engineering (sin/cos, cents, interactions)
    Spark->>Spark: Align Pandas Categoricals against booster.pandas_categorical
    Spark->>Spark: Compute P(isFraud) per record
    
    Note over Spark,KAlert: Decision & Dual-Routing Phase (<25ms)
    alt Fraud Detected: P(isFraud) >= 0.10
        Spark->>KAlert: Produce Alert Payload (Key = tx_id, Value = JSON)
    else Legitimate Transaction: P(isFraud) < 0.10
        Spark->>Spark: Mark action = 'APPROVE'
    end
    
    Spark->>BQ: Direct append complete batch to transactions_log
    
    Note over Spark,Prom: Telemetry Scraping (Asynchronous @ 5s)
    Spark->>Prom: Expose Latency, Counts, Score Distribution (:8000)
    Prom->>Prom: Scrape Spark & Kafka Exporter
```

---

## 3. Offline Feature Selection & Online Parity Pipeline

```mermaid
flowchart LR
    subgraph RAW["Raw IEEE-CIS Dataset"]
        R1["590,540 Transactions"]
        R2["434 Raw Features"]
        R3["339 Proprietary V1-V339 Features"]
        R4["38 Obscured id_01 - id_38 Masks"]
    end

    subgraph PRUNING["Feature Pruning (Anti-Pattern Elimination)"]
        DROP["Drop V-columns & id-masks<br/>(Cannot calculate in real-time under 50ms)"]
        RETAIN["Retain Primitives:<br/>Amt, ProductCD, card1-6, addr1-2,<br/>P/R_emaildomain, DeviceType, C1-14, D1"]
    end

    subgraph ENG["Reproducible Feature Engineering (30 Features)"]
        F1["Cyclical Temporal:<br/>Hour_sin, Hour_cos, Hour_of_Day, Day_of_Week"]
        F2["Monetary Transforms:<br/>Amt_log1p, Amt_cents, Is_round_dollar"]
        F3["Entity Interaction:<br/>card1_addr1 (Composite String)"]
        F4["Domain Match:<br/>email_match (P == R)"]
        F5["Categorical Columns (7):<br/>Exact pandas Category Dtypes"]
    end

    subgraph PARITY["Online Streaming Parity"]
        STREAM["Spark Vectorized Pandas UDF"]
        CATEGORIES["Enforce booster.pandas_categorical<br/>(Unknown categories mapped to NaN)"]
        MODEL["LightGBM Booster Inference<br/>(P99 < 5ms)"]
    end

    RAW --> PRUNING
    R3 -.->|"Destroy Streaming Latency"| DROP
    R4 -.->|"Non-Reproducible"| DROP
    PRUNING --> RETAIN
    RETAIN --> ENG
    ENG -->|"train_features.parquet"| STREAM
    STREAM --> CATEGORIES
    CATEGORIES --> MODEL
```

---

## 4. Decision Boundary & Financial Cost-Matrix Optimization

```mermaid
flowchart TD
    SCORE["Incoming Transaction Fraud Probability: P"]
    THRESH{"Decision Threshold<br/>tau* = 0.10"}
    
    SCORE --> THRESH
    
    subgraph ROUTE_ALERT["High-Risk Path"]
        BLOCK["Tag: BLOCK_AND_ALERT<br/>is_fraud_alert = 1"]
        K_ALERT["Publish to Kafka: transactions.alerts"]
        OPS["Downstream Incident Response<br/>& Real-time User SMS/2FA Challenge"]
        
        BLOCK --> K_ALERT
        K_ALERT --> OPS
    end
    
    subgraph ROUTE_APPROVE["Standard Path"]
        ALLOW["Tag: APPROVE<br/>is_fraud_alert = 0"]
        GATEWAY["Payment Gateway Settlement"]
        
        ALLOW --> GATEWAY
    end
    
    subgraph ARCHIVE["Dual-Sink Data Warehouse"]
        BQ["Google Cloud BigQuery<br/>fraud_analytics.transactions_log"]
        DRIFT["Model Drift & Continuous Training Pipeline"]
        
        BQ --> DRIFT
    end
    
    THRESH -->|">= 0.10 (Optimized for Cost Matrix)"| BLOCK
    THRESH -->|"< 0.10"| ALLOW
    BLOCK --> BQ
    ALLOW --> BQ

    classDef alert fill:#dc2626,stroke:#991b1b,color:#ffffff;
    classDef approve fill:#16a34a,stroke:#15803d,color:#ffffff;
    classDef sink fill:#2563eb,stroke:#1d4ed8,color:#ffffff;
    
    class BLOCK,K_ALERT,OPS alert;
    class ALLOW,GATEWAY approve;
    class BQ,DRIFT sink;
```

---

## 5. Observability & Telemetry Scrape Topology

```mermaid
flowchart LR
    subgraph SOURCES["Telemetry Producers"]
        K_BROKER["Kafka Broker (:9092)"]
        SPARK_DRIVER["Spark Streaming Driver (:8000)"]
    end

    subgraph AGENTS["Exporters & Endpoints"]
        K_EXP["Kafka Exporter (:9308)<br/>Metrics: kafka_consumergroup_lag,<br/>kafka_topic_partitions"]
        S_HTTP["Embedded HTTP Server (:8000/metrics)<br/>Metrics: transactions_processed_total,<br/>fraud_alerts_total,<br/>spark_batch_processing_seconds,<br/>fraud_probability_distribution"]
    end

    subgraph TSDB["Time-Series Collection"]
        PROM["Prometheus Server (:9090)<br/>Scrape Interval: 5 seconds<br/>Storage: TSDB Volume"]
    end

    subgraph VIZ["Visualization & Alerting"]
        GRAF["Grafana Dashboard (:3000)<br/>- Consumer Group Lag Panel<br/>- Batch Duration vs 50ms SLA<br/>- Fraud Alert Rate (req/s)<br/>- Model Probability Drift"]
    end

    K_BROKER --> K_EXP
    SPARK_DRIVER --> S_HTTP
    K_EXP -->|"HTTP GET /metrics"| PROM
    S_HTTP -->|"HTTP GET /metrics"| PROM
    PROM -->|"PromQL Queries"| GRAF
```

---

## 6. Data Entity Relationship & Schema Mapping

```mermaid
erDiagram
    RAW_TRANSACTION {
        string transaction_id PK
        string user_id
        double amount
        string currency
        string timestamp
        string product_cd
        double card1
        double card2
        string card4
        string card6
        double addr1
        string p_emaildomain
        string r_emaildomain
        string device_type
        double c1
        double c2
        double c5
        double c6
        double c13
        double c14
        double d1
    }

    ENGINEERED_FEATURES {
        double TransactionAmt
        string ProductCD
        double card1
        double card2
        string card4
        string card6
        double addr1
        double addr2
        string P_emaildomain
        string R_emaildomain
        string DeviceType
        double Hour_sin
        double Hour_cos
        double Amt_log1p
        double Amt_cents
        int Is_round_dollar
        string card1_addr1
        int email_match
        int null_count
    }

    SCORED_EVENT {
        string transaction_id PK
        double amount
        string product_cd
        double fraud_probability
        string action
        int is_fraud_alert
        timestamp processed_at
    }

    KAFKA_ALERT {
        string key PK "transaction_id"
        string value "JSON String of Scored Event"
    }

    BIGQUERY_RECORD {
        string transaction_id PK
        double amount
        string product_cd
        double fraud_probability
        int is_fraud_alert
        string action
        timestamp ingestion_time
    }

    RAW_TRANSACTION ||--|| ENGINEERED_FEATURES : "Vectorized UDF Feature Derivation"
    ENGINEERED_FEATURES ||--|| SCORED_EVENT : "LightGBM Scoring (tau* = 0.10)"
    SCORED_EVENT ||--o| KAFKA_ALERT : "Filtered (is_fraud_alert == 1)"
    SCORED_EVENT ||--|| BIGQUERY_RECORD : "Batch Direct Write (All records)"
```
