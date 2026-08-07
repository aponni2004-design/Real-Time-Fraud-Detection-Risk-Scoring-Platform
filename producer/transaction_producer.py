from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime, timezone


KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "transactions"

MERCHANT_CATEGORIES = [
    "Grocery",
    "Fuel",
    "Restaurant",
    "Electronics",
    "Shopping",
    "Travel",
    "Entertainment",
    "Healthcare"
]

PAYMENT_METHODS = [
    "UPI",
    "Credit Card",
    "Debit Card",
    "Net Banking"
]

CITIES = [
    "Chennai",
    "Bengaluru",
    "Hyderabad",
    "Mumbai",
    "Delhi",
    "Pune",
    "Coimbatore",
    "Kolkata"
]


producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


def generate_transaction(transaction_number):
    customer_number = random.randint(1, 1000)
    merchant_number = random.randint(1, 500)
    device_number = random.randint(1, 1500)

    transaction = {
        "transaction_id": f"TXN{transaction_number:08d}",
        "customer_id": f"CUST{customer_number:06d}",
        "amount": round(random.uniform(100, 50000), 2),
        "currency": "INR",
        "merchant_id": f"MERCH{merchant_number:05d}",
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "payment_method": random.choice(PAYMENT_METHODS),
        "device_id": f"DEV{device_number:06d}",
        "city": random.choice(CITIES),
        "country": "India",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    return transaction


print("Starting transaction producer...")
print(f"Kafka server: {KAFKA_SERVER}")
print(f"Topic: {TOPIC_NAME}")


for i in range(1, 101):
    transaction = generate_transaction(i)

    producer.send(
        TOPIC_NAME,
        value=transaction
    )

    print(
        f"Sent {transaction['transaction_id']} | "
        f"Customer: {transaction['customer_id']} | "
        f"Amount: ₹{transaction['amount']}"
    )

    time.sleep(0.1)


producer.flush()
producer.close()

print("\n100 transactions sent successfully!")