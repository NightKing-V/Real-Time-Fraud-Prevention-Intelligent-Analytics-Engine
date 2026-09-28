import os
import time
import warnings
import numpy as np
import pandas as pd
import mlflow
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
from pyspark.sql.functions import from_json, col, struct, when, pandas_udf

# Suppress noisy warnings
warnings.filterwarnings("ignore")

# Initialize Prometheus Metrics Server on Port 8000
try:
    from prometheus_client import start_http_server, Counter, Gauge, Histogram
    start_http_server(8000)
    print("Prometheus metrics server live on port 8000 (/metrics)")
    PROMETHEUS_AVAILABLE = True
except Exception as prom_err:
    print(f"[WARN] Could not initialize prometheus_client on port 8000: {prom_err}")
    PROMETHEUS_AVAILABLE = False

if PROMETHEUS_AVAILABLE:
    TX_TOTAL = Counter('transactions_processed_total', 'Total transactions scored by the engine')
    FRAUD_ALERTS = Counter('fraud_alerts_total', 'Total fraud alerts triggered (action = BLOCK_AND_ALERT)')
    BATCH_PROCESSING_TIME = Gauge('spark_batch_processing_seconds', 'Processing time per micro-batch in seconds')
    BATCH_SIZE = Gauge('spark_batch_records_count', 'Number of transactions in the current micro-batch')
    SCORE_DIST = Histogram(
        'fraud_probability_distribution',
        'Distribution of real-time fraud probability scores',
        buckets=[0.01, 0.05, 0.10, 0.20, 0.35, 0.50, 0.75, 0.90, 1.00]
    )
    BQ_FAILURES = Counter('bigquery_sync_failures_total', 'Total BigQuery append failures')

# 1. Initialize the Spark Session
spark = SparkSession.builder \
    .appName("FraudDetectionInference") \
    .master("local[*]") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark Session created successfully.")

# 2. Define MLflow Booster Singleton Loader for PySpark Workers
_BOOSTER = None
_CATEGORIES = None
_FEATURE_NAMES = None

def get_booster():
    """Lazily loads the production LightGBM Booster artifact inside the worker."""
    global _BOOSTER, _CATEGORIES, _FEATURE_NAMES
    if _BOOSTER is None:
        tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
        mlflow.set_tracking_uri(tracking_uri)
        print(f"Connecting to MLflow at {tracking_uri}...")
        loaded = mlflow.lightgbm.load_model("models:/FraudDetectionModel/Production")
        _BOOSTER = loaded.booster_
        _CATEGORIES = _BOOSTER.pandas_categorical
        _FEATURE_NAMES = loaded.feature_name_
        print(f"Loaded 'Production' FraudDetectionModel with {len(_FEATURE_NAMES)} features and {len(_CATEGORIES)} categorical encodings.")
    return _BOOSTER, _CATEGORIES, _FEATURE_NAMES

CAT_COLS = ['ProductCD', 'card4', 'card6', 'P_emaildomain', 'R_emaildomain', 'DeviceType', 'card1_addr1']

@pandas_udf(DoubleType())
def predict_fraud_pandas(pdf: pd.DataFrame) -> pd.Series:
    """
    Vectorized Arrow Pandas UDF:
    - Derives required temporal, monetary, and interaction features in real-time (<5ms)
    - Enforces exact LightGBM categorical encoding metadata from the MLflow artifact
    - Returns fraud probability score P(isFraud = 1)
    """
    booster, cats, feat_names = get_booster()
    
    df = pd.DataFrame(index=pdf.index)
    
    # 1. Base Primitives
    df['TransactionAmt'] = pd.to_numeric(pdf['amount'], errors='coerce').fillna(0.0).astype(float)
    df['ProductCD'] = pdf['product_cd'].astype(str)
    df['card1'] = pd.to_numeric(pdf['card1'], errors='coerce').fillna(0.0).astype(float)
    df['card2'] = pd.to_numeric(pdf['card2'], errors='coerce').fillna(0.0).astype(float)
    df['card4'] = pdf['card4'].astype(str)
    df['card6'] = pdf['card6'].astype(str)
    df['addr1'] = pd.to_numeric(pdf['addr1'], errors='coerce').fillna(0.0).astype(float)
    df['addr2'] = 87.0  # IEEE-CIS standard country/region code
    df['P_emaildomain'] = pdf['p_emaildomain'].astype(str)
    df['R_emaildomain'] = pdf['r_emaildomain'].astype(str)
    df['C1'] = pd.to_numeric(pdf['c1'], errors='coerce').fillna(0.0).astype(float)
    df['C2'] = pd.to_numeric(pdf['c2'], errors='coerce').fillna(0.0).astype(float)
    df['C5'] = pd.to_numeric(pdf['c5'], errors='coerce').fillna(0.0).astype(float)
    df['C6'] = pd.to_numeric(pdf['c6'], errors='coerce').fillna(0.0).astype(float)
    df['C13'] = pd.to_numeric(pdf['c13'], errors='coerce').fillna(0.0).astype(float)
    df['C14'] = pd.to_numeric(pdf['c14'], errors='coerce').fillna(0.0).astype(float)
    df['D1'] = pd.to_numeric(pdf['d1'], errors='coerce').fillna(0.0).astype(float)
    df['DeviceType'] = pdf['device_type'].astype(str)

    # 2. Temporal Features
    dt = pd.to_datetime(pdf['timestamp'], errors='coerce')
    hour = dt.dt.hour.fillna(12).astype(int)
    day = dt.dt.dayofweek.fillna(3).astype(int)
    df['Hour_of_Day'] = hour
    df['Day_of_Week'] = day
    df['Hour_sin'] = np.sin(2 * np.pi * hour / 24.0).astype(np.float32)
    df['Hour_cos'] = np.cos(2 * np.pi * hour / 24.0).astype(np.float32)

    # 3. Monetary Engineered Features
    amt_cents = (df['TransactionAmt'] - df['TransactionAmt'].astype(int)).round(3)
    is_round = (amt_cents == 0.0).astype(int)
    df['Amt_Cents'] = amt_cents
    df['Amt_cents'] = amt_cents.astype(np.float32)
    df['Is_Round_Dollar'] = is_round
    df['Is_round_dollar'] = is_round
    df['Amt_log1p'] = np.log1p(np.maximum(0.0, df['TransactionAmt'])).astype(np.float32)

    # 4. Entity & Domain Interactions
    df['card1_addr1'] = df['card1'].astype(str) + "_" + df['addr1'].astype(str)
    df['email_match'] = ((df['P_emaildomain'] != "") & (df['P_emaildomain'] == df['R_emaildomain'])).astype(int)
    df['null_count'] = 0

    # 5. Cast Categorical Columns against the Trained Booster Categories
    for col_name, cat_list in zip(CAT_COLS, cats):
        df[col_name] = pd.Categorical(df[col_name], categories=cat_list)

    # 6. Reorder strictly according to model feature schema
    df = df[feat_names]

    # 7. Predict Probability Score P(isFraud = 1)
    preds = booster.predict(df)
    return pd.Series(preds, index=pdf.index)

# 3. Define the Incoming Kafka JSON Schema
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("country", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("product_cd", StringType(), True),
    StructField("card1", DoubleType(), True),
    StructField("card2", DoubleType(), True),
    StructField("card4", StringType(), True),
    StructField("card6", StringType(), True),
    StructField("p_emaildomain", StringType(), True),
    StructField("r_emaildomain", StringType(), True),
    StructField("addr1", DoubleType(), True),
    StructField("device_type", StringType(), True),
    StructField("c1", DoubleType(), True),
    StructField("c2", DoubleType(), True),
    StructField("c5", DoubleType(), True),
    StructField("c6", DoubleType(), True),
    StructField("c13", DoubleType(), True),
    StructField("c14", DoubleType(), True),
    StructField("d1", DoubleType(), True)
])

# 4. Connect to the Kafka Topic
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker:9092") \
    .option("subscribe", "transactions.incoming") \
    .option("startingOffsets", "latest") \
    .load()

# Parse binary Kafka payload
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), transaction_schema).alias("data")) \
    .select("data.*")

# 5. Execute Real-Time Inference
raw_input_cols = [
    'amount', 'timestamp', 'product_cd', 'card1', 'card2', 'card4', 'card6',
    'p_emaildomain', 'r_emaildomain', 'addr1', 'device_type',
    'c1', 'c2', 'c5', 'c6', 'c13', 'c14', 'd1'
]

scored_stream = parsed_stream.withColumn(
    "fraud_probability",
    predict_fraud_pandas(struct(*[col(c) for c in raw_input_cols]))
)

# 6. Apply Optimal Financial Threshold (0.10) & Routing Tag
final_stream = scored_stream.withColumn(
    "is_fraud_alert",
    (col("fraud_probability") >= 0.10).cast("int")
).withColumn(
    "action",
    when(col("fraud_probability") >= 0.10, "BLOCK_AND_ALERT").otherwise("APPROVE")
)

# 7. Dual-Sink Routing (BigQuery & Kafka Alerts) with Prometheus Telemetry

def route_micro_batch(batch_df, batch_id):
    t_start = time.time()
    
    # 0. Measure batch size and update transaction metrics
    batch_count = batch_df.count()
    if batch_count == 0:
        return

    if PROMETHEUS_AVAILABLE:
        BATCH_SIZE.set(batch_count)
        TX_TOTAL.inc(batch_count)
        try:
            scores = [row.fraud_probability for row in batch_df.select("fraud_probability").collect()]
            for score in scores:
                if score is not None:
                    SCORE_DIST.observe(float(score))
        except Exception as metric_err:
            pass

    # 1. Write the complete transaction log to BigQuery for future MLOps training
    try:
        batch_df.write \
            .format("bigquery") \
            .option("table", "white-cedar-510007-k2.fraud_analytics.transactions_log") \
            .option("writeMethod", "direct") \
            .mode("append") \
            .save()
        print(f"[INFO] Successfully written batch {batch_id} ({batch_count} records) to BigQuery.")
    except Exception as bq_err:
        if PROMETHEUS_AVAILABLE:
            BQ_FAILURES.inc()
        print(f"[WARN] BigQuery write failed for batch {batch_id}: {bq_err}")

    # 2. Isolate blocked transactions and push them to the Kafka alerts topic
    alerts_df = batch_df.filter(col("is_fraud_alert") == 1)
    alert_count = alerts_df.count()
    
    if alert_count > 0:
        if PROMETHEUS_AVAILABLE:
            FRAUD_ALERTS.inc(alert_count)
        
        kafka_alerts_df = alerts_df.selectExpr(
            "CAST(transaction_id AS STRING) AS key", 
            "to_json(struct(*)) AS value"
        )
        
        kafka_alerts_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka-broker:9092") \
            .option("topic", "transactions.alerts") \
            .save()
        print(f"[ALERT] Routed {alert_count} fraud alerts to Kafka topic 'transactions.alerts'")

    # 3. Record batch processing latency
    duration = time.time() - t_start
    if PROMETHEUS_AVAILABLE:
        BATCH_PROCESSING_TIME.set(duration)

# Execute the dual-sink routing on the live stream
query = final_stream.writeStream \
    .foreachBatch(route_micro_batch) \
    .option("checkpointLocation", "/app/checkpoints/dual_sink") \
    .start()

query.awaitTermination()