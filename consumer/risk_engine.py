import json
import time
import psycopg2
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
import fraud_rules

# PostgreSQL Connection
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="fraud_db",
    user="admin",
    password="password"
)
conn.autocommit = True
cursor = conn.cursor()

# Kafka Consumer & Producer
consumer = KafkaConsumer(
    "transactions.raw",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="fraud-risk-engine-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def evaluate_transaction(tx: dict) -> tuple[str, list[str], float]:
    start_time = time.perf_counter()
    triggered_rules = []

    card_id = tx["card_id"]
    dt = datetime.fromisoformat(tx["timestamp"])
    epoch_time = dt.timestamp()

    # Rule 1: Velocity check
    if fraud_rules.check_velocity(card_id, epoch_time):
        triggered_rules.append("velocity")

    # Rule 2: Impossible travel check
    if fraud_rules.check_impossible_travel(card_id, tx["latitude"], tx["longitude"], epoch_time):
        triggered_rules.append("impossible_travel")

    # Rule 3: High-Risk MCC check
    if fraud_rules.check_high_risk_mcc(tx["merchant_category_code"], tx["amount"]):
        triggered_rules.append("high_risk_mcc")

    latency_ms = (time.perf_counter() - start_time) * 1000.0
    decision = "flagged" if len(triggered_rules) > 0 else "approved"

    return decision, triggered_rules, latency_ms


def persist_transaction(tx: dict, decision: str, triggered_rules: list[str], latency_ms: float):
    # Insert into PostgreSQL transactions table
    cursor.execute(
        """
        INSERT INTO transactions (
            transaction_id, card_id, amount, merchant_id, merchant_category_code,
            latitude, longitude, timestamp, is_fraud, risk_decision,
            triggered_rules, scoring_latency_ms
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id) DO NOTHING;
        """,
        (
            tx["transaction_id"],
            tx["card_id"],
            tx["amount"],
            tx["merchant_id"],
            tx["merchant_category_code"],
            tx["latitude"],
            tx["longitude"],
            tx["timestamp"],
            tx.get("is_fraud", False),
            decision,
            triggered_rules,
            latency_ms,
        ),
    )

    # If flagged, also record an incident in fraud_alerts
    if decision == "flagged":
        cursor.execute(
            """
            INSERT INTO fraud_alerts (transaction_id, triggered_rules)
            VALUES (%s, %s);
            """,
            (tx["transaction_id"], triggered_rules),
        )


def start_engine():
    print("Risk Engine working. Consuming from 'transactions.raw'...")

    for message in consumer:
        tx = message.value
        decision, triggered_rules, latency_ms = evaluate_transaction(tx)

        # Route to respective output Kafka topic
        output_topic = "transactions.flagged" if decision == "flagged" else "transactions.approved"
        producer.send(output_topic, value=tx)

        # Persist audit record to Postgres
        persist_transaction(tx, decision, triggered_rules, latency_ms)

        # Logging
        if decision == "flagged":
            print(f"[FLAGGED] Card {tx['card_id']} | Amount: ${tx['amount']:.2f} | Rules: {triggered_rules} | Latency: {latency_ms:.2f}ms")
        else:
            print(f"[APPROVED] Card {tx['card_id']} | Amount: ${tx['amount']:.2f} | Latency: {latency_ms:.2f}ms")


if __name__ == "__main__":
    start_engine()