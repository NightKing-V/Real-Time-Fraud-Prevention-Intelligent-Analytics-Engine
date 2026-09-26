import json
import time
import random
import requests
from faker import Faker
from datetime import datetime, timezone

fake = Faker()

# The NiFi endpoint we will configure to listen for these payloads
NIFI_ENDPOINT = "http://localhost:8082/contentListener"

def generate_transaction():
    """Generates a single synthetic checkout event."""
    return {
        "transaction_id": f"tx_{fake.uuid4()[:8]}",
        "user_id": f"usr_{random.randint(1000, 9999)}",
        "amount": round(random.uniform(5.00, 1500.00), 2),
        "currency": "USD",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "merchant_category": random.choice(["electronics", "clothing", "digital_goods", "food", "travel"]),
        "ip_address": fake.ipv4(),
        "country": fake.country_code(),
        "payment_method": random.choice(["credit_card", "paypal", "crypto"]),
        "device_id": f"dev_{random.randint(10000, 99999)}"
    }

def run_simulator(transactions_per_second=10):
    """Fires transactions at NiFi at a controlled rate."""
    sleep_time = 1.0 / transactions_per_second
    print(f"Starting simulated stream at {transactions_per_second} tx/sec...")
    
    while True:
        payload = generate_transaction()
        try:
            # POST the JSON payload to Apache NiFi
            response = requests.post(
                NIFI_ENDPOINT, 
                json=payload, 
                headers={"Content-Type": "application/json"}
            )
            print(f"Sent {payload['transaction_id']} - Amount: ${payload['amount']} - Status: {response.status_code}")
        except requests.exceptions.ConnectionError:
            print("Failed to connect to NiFi. Is the ListenHTTP processor running?")
        
        time.sleep(sleep_time)

if __name__ == "__main__":
    # Start small to verify the pipeline
    run_simulator(transactions_per_second=10)