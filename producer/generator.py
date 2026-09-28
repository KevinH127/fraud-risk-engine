import json
import random
import time
import uuid
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer
from transaction_schema import Transaction

fake = Faker()

# Common Merchant Category Codes (MCC)
MERCHANTS = [
    {"id": "M_GROCERY_01", "name": "Metro Supermarket", "mcc": 5411},
    {"id": "M_COFFEE_02",  "name": "Tim Hortons",        "mcc": 5814},
    {"id": "M_GAS_03",     "name": "Petro-Canada",       "mcc": 5541},
    {"id": "M_RETAIL_04",  "name": "Best Buy",           "mcc": 5732},
    {"id": "M_CRYPTO_05",  "name": "Coinbase Exchange",  "mcc": 6051},  # High risk
    {"id": "M_GAMBLING_06","name": "BetMGM Online",      "mcc": 7995},  # High risk
]

# Fixed pool of 200 cardholders to allow realistic repeat swipes
CARD_POOL = [f"CARD_{fake.md5()[:12].upper()}" for _ in range(200)]

# City coordinate anchors for travel simulation
CITIES = {
    "Toronto": (43.6532, -79.3832),
    "Montreal": (45.5017, -73.5673),
    "Vancouver": (49.2827, -123.1207),
    "London_UK": (51.5074, -0.1278),  # Used for impossible travel
}


def generate_legit_transaction(card_id: str) -> Transaction:
    merchant = random.choices(
        MERCHANTS, 
        weights=[0.35, 0.35, 0.15, 0.10, 0.03, 0.02]
    )[0]
    
    # Coordinates centered near Toronto with slight random jitter
    base_lat, base_lon = CITIES["Toronto"]
    lat = base_lat + random.uniform(-0.08, 0.08)
    lon = base_lon + random.uniform(-0.08, 0.08)
    
    # Exponential distribution for realistic spending (average ~$45)
    amount = round(random.expovariate(1 / 45.0) + 2.0, 2)

    return Transaction(
        transaction_id=str(uuid.uuid4()),
        card_id=card_id,
        amount=amount,
        merchant_id=merchant["id"],
        merchant_category_code=merchant["mcc"],
        latitude=lat,
        longitude=lon,
        timestamp=datetime.now(timezone.utc),
        is_fraud=False,
    )


def generate_fraud_transaction(card_id: str, fraud_type: str) -> Transaction:
    tx = generate_legit_transaction(card_id)
    tx.is_fraud = True
    tx.fraud_type = fraud_type

    if fraud_type == "impossible_travel":
        # Sudden jump across the Atlantic to London UK
        london_lat, london_lon = CITIES["London_UK"]
        tx.latitude = london_lat + random.uniform(-0.02, 0.02)
        tx.longitude = london_lon + random.uniform(-0.02, 0.02)
        tx.amount = round(random.uniform(400.0, 1500.0), 2)

    elif fraud_type == "high_mcc":
        # Abnormally large transaction at a high-risk merchant
        tx.merchant_id = "M_CRYPTO_05"
        tx.merchant_category_code = 6051
        tx.amount = round(random.uniform(1200.0, 5000.0), 2)

    elif fraud_type == "velocity":
        # High value rapid spend
        tx.amount = round(random.uniform(250.0, 800.0), 2)

    return tx


def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    print("🚀 Transaction generator started! Streaming into 'transactions.raw'...")
    print("Press Ctrl + C to stop.\n")

    try:
        while True:
            card_id = random.choice(CARD_POOL)
            roll = random.random()

            if roll < 0.05:
                # 5% Velocity Fraud: Rapid burst of 5 transactions on the same card
                print(f"⚠️  [INJECTING] Velocity Fraud burst for {card_id}")
                for _ in range(5):
                    tx = generate_fraud_transaction(card_id, "velocity")
                    producer.send("transactions.raw", key=card_id, value=tx.to_dict())
                    time.sleep(0.05)

            elif roll < 0.08:
                # 3% Impossible Travel Fraud: Toronto swipe followed by instant London swipe
                t1 = generate_legit_transaction(card_id)
                producer.send("transactions.raw", key=card_id, value=t1.to_dict())
                time.sleep(0.1)

                print(f"✈️  [INJECTING] Impossible Travel Fraud for {card_id}")
                t2 = generate_fraud_transaction(card_id, "impossible_travel")
                producer.send("transactions.raw", key=card_id, value=t2.to_dict())

            elif roll < 0.10:
                # 2% High-Risk MCC Fraud
                print(f"💳 [INJECTING] High-Risk MCC Fraud for {card_id}")
                tx = generate_fraud_transaction(card_id, "high_mcc")
                producer.send("transactions.raw", key=card_id, value=tx.to_dict())

            else:
                # 90% Normal Everyday Transactions
                tx = generate_legit_transaction(card_id)
                producer.send("transactions.raw", key=card_id, value=tx.to_dict())

            # Emit ~5 to 10 transactions per second
            time.sleep(random.uniform(0.1, 0.2))

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    run_producer()