from kafka import KafkaProducer
import json
import random
import time
import uuid
from datetime import datetime, timezone

# -----------------------------------------
# Capacity Test Configuration
# -----------------------------------------

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "transactions_capacity_10k"
TOTAL_TRANSACTIONS = 10_000

# -----------------------------------------
# Reference data
# -----------------------------------------

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

# -----------------------------------------
# Kafka producer
# -----------------------------------------

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

# -----------------------------------------
# Generate transaction ID
# -----------------------------------------

def generate_transaction_id():
    return f"CAP10K{uuid.uuid4().hex.upper()}"


# -----------------------------------------
# Generate transaction
# -----------------------------------------

def generate_transaction():

    customer_number = random.randint(1, 1000)
    merchant_number = random.randint(1, 500)
    device_number = random.randint(1, 1500)

    transaction_type = random.choices(
        ["NORMAL", "HIGH_VALUE", "FRAUD"],
        weights=[85, 10, 5],
        k=1
    )[0]

    if transaction_type == "NORMAL":
        amount = round(random.uniform(100, 30000), 2)

    elif transaction_type == "HIGH_VALUE":
        amount = round(random.uniform(40001, 70000), 2)

    else:
        amount = round(random.uniform(75001, 150000), 2)

    return {
        "transaction_id": generate_transaction_id(),
        "customer_id": f"CUST{customer_number:06d}",
        "amount": amount,
        "currency": "INR",
        "merchant_id": f"MERCH{merchant_number:05d}",
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "payment_method": random.choice(PAYMENT_METHODS),
        "device_id": f"DEV{device_number:06d}",
        "city": random.choice(CITIES),
        "country": "India",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


# -----------------------------------------
# Start capacity test
# -----------------------------------------

print("==========================================")
print("10K Kafka Capacity Test")
print("==========================================")
print(f"Kafka server : {KAFKA_SERVER}")
print(f"Topic        : {TOPIC_NAME}")
print(f"Transactions : {TOTAL_TRANSACTIONS}")
print("==========================================")

start_time = time.perf_counter()

# -----------------------------------------
# Send transactions
# -----------------------------------------

for i in range(1, TOTAL_TRANSACTIONS + 1):

    transaction = generate_transaction()

    producer.send(
        TOPIC_NAME,
        value=transaction
    )

    if i % 1000 == 0:
        print(f"Sent {i:,} transactions")


# -----------------------------------------
# Flush remaining messages
# -----------------------------------------

producer.flush()

end_time = time.perf_counter()

producer.close()

# -----------------------------------------
# Calculate producer throughput
# -----------------------------------------

elapsed_time = end_time - start_time
throughput = TOTAL_TRANSACTIONS / elapsed_time

print("==========================================")
print("10K Capacity Test Completed")
print("==========================================")
print(f"Transactions sent : {TOTAL_TRANSACTIONS:,}")
print(f"Elapsed time      : {elapsed_time:.2f} seconds")
print(f"Producer throughput: {throughput:,.2f} records/sec")
print("==========================================")