from kafka import KafkaProducer
import json
import random
import time
import uuid
from datetime import datetime, timezone

# -----------------------------------------
# 1. Kafka configuration
# -----------------------------------------

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "transactions"

# -----------------------------------------
# 2. Transaction reference data
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
# 3. Create Kafka producer
# -----------------------------------------

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

# -----------------------------------------
# 4. Generate transaction
# -----------------------------------------
def generate_transaction_id():
    return f"TXN{uuid.uuid4().hex.upper()}"
def generate_transaction(transaction_number):

    customer_number = random.randint(1, 1000)
    merchant_number = random.randint(1, 500)
    device_number = random.randint(1, 1500)

    # -----------------------------------------
    # Decide transaction type
    #
    # 85% = Normal
    # 10% = High-value
    # 5%  = Fraud-like
    # -----------------------------------------

    transaction_type = random.choices(
        ["NORMAL", "HIGH_VALUE", "FRAUD"],
        weights=[85, 10, 5],
        k=1
    )[0]

    # -----------------------------------------
    # Normal transaction
    # -----------------------------------------

    if transaction_type == "NORMAL":

        amount = round(random.uniform(100, 30000), 2)

        device_id = f"DEV{device_number:06d}"
        merchant_id = f"MERCH{merchant_number:05d}"

    # -----------------------------------------
    # High-value transaction
    # -----------------------------------------

    elif transaction_type == "HIGH_VALUE":

        amount = round(random.uniform(40001, 70000), 2)

        device_id = f"DEV{device_number:06d}"
        merchant_id = f"MERCH{merchant_number:05d}"

    # -----------------------------------------
    # Fraud-like transaction
    # -----------------------------------------

    else:

        # Amount above 75,000 will trigger
        # both high-value rules in Gold.
        amount = round(random.uniform(75001, 150000), 2)

        device_id = f"DEV{device_number:06d}"
        merchant_id = f"MERCH{merchant_number:05d}"

    # -----------------------------------------
    # Create transaction
    # -----------------------------------------

    transaction = {
        "transaction_id": generate_transaction_id(),
        "customer_id": f"CUST{customer_number:06d}",
        "amount": amount,
        "currency": "INR",
        "merchant_id": merchant_id,
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "payment_method": random.choice(PAYMENT_METHODS),
        "device_id": device_id,
        "city": random.choice(CITIES),
        "country": "India",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    return transaction


# -----------------------------------------
# 5. Start producer
# -----------------------------------------

print("===================================")
print("Real-Time Fraud Transaction Producer")
print("===================================")
print(f"Kafka server : {KAFKA_SERVER}")
print(f"Topic        : {TOPIC_NAME}")
print("Generating   : 1000 transactions")
print("===================================")

# -----------------------------------------
# 6. Generate and send transactions
# -----------------------------------------

for i in range(1, 1001):

    transaction = generate_transaction(i)

    producer.send(
        TOPIC_NAME,
        value=transaction
    )

    print(
        f"Sent {transaction['transaction_id']} | "
        f"Customer: {transaction['customer_id']} | "
        f"Amount: ₹{transaction['amount']:.2f}"
    )

    # Small delay to simulate real-time events
    time.sleep(0.05)

# -----------------------------------------
# 7. Flush and close
# -----------------------------------------

producer.flush()
producer.close()

print("===================================")
print("1000 transactions sent successfully!")
print("===================================")
