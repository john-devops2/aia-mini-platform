# producer.py
# This script connects to Kafka and sends 5 fake order messages
# into the 'orders' topic — one message every second

from kafka import KafkaProducer
import json
import time

# Step 1 — Connect to Kafka broker
# 29092 is our external port — what your laptop uses to reach Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Connected to Kafka. Sending messages...\n")

# Step 2 — Define 5 fake orders
orders = [
    {"order_id": "ORD-001", "customer": "alice", "amount": 99.50,  "status": "placed"},
    {"order_id": "ORD-002", "customer": "bob",   "amount": 149.00, "status": "placed"},
    {"order_id": "ORD-003", "customer": "alice", "amount": 35.75,  "status": "placed"},
    {"order_id": "ORD-004", "customer": "carol", "amount": 220.00, "status": "placed"},
    {"order_id": "ORD-005", "customer": "dave",  "amount": 67.25,  "status": "placed"},
]

# Step 3 — Send each order to the 'orders' topic
for order in orders:
    producer.send('orders', value=order)
    print(f"Sent: {order['order_id']} | customer: {order['customer']} | amount: ${order['amount']}")
    time.sleep(1)

# Step 4 — Make sure all messages are delivered before exiting
producer.flush()
print("\nAll messages sent to Kafka topic: orders")