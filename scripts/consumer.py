# consumer.py
# This script reads messages from the Kafka 'orders' topic
# and writes each message into a PostgreSQL table
# This is the pipeline — data moves from Kafka into storage

from kafka import KafkaConsumer
import psycopg2
import json

# ─────────────────────────────────────────
# Step 1 — Connect to PostgreSQL
# This is our local database — stores consumed messages permanently
# ─────────────────────────────────────────
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="aiadb",
    user="aiauser",
    password="aiapass"
)
cursor = conn.cursor()
print("Connected to PostgreSQL\n")

# ─────────────────────────────────────────
# Step 2 — Create the orders table if it doesn't exist yet
# This is like creating a BigQuery table before loading data
# ─────────────────────────────────────────
cursor.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id   TEXT PRIMARY KEY,
        customer   TEXT,
        amount     NUMERIC,
        status     TEXT,
        created_at TIMESTAMP DEFAULT NOW()
    )
""")
conn.commit()
print("Table 'orders' is ready\n")

# ─────────────────────────────────────────
# Step 3 — Connect to Kafka and start consuming
# group_id identifies this consumer — Kafka tracks its progress
# auto_offset_reset='earliest' means start from the first message
# ─────────────────────────────────────────
consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:29092',
    group_id='orders-consumer-group',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    consumer_timeout_ms=10000
)
print("Connected to Kafka. Reading messages...\n")

# ─────────────────────────────────────────
# Step 4 — Read each message and write to PostgreSQL
# ─────────────────────────────────────────
count = 0
for message in consumer:
    order = message.value

    cursor.execute("""
        INSERT INTO orders (order_id, customer, amount, status)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (order_id) DO NOTHING
    """, (order['order_id'], order['customer'], order['amount'], order['status']))

    conn.commit()
    count += 1

    print(f"Consumed: {order['order_id']} | partition: {message.partition} | offset: {message.offset}")

# ─────────────────────────────────────────
# Step 5 — Done
# ─────────────────────────────────────────
print(f"\nDone. {count} messages written to PostgreSQL table 'orders'")
cursor.close()
conn.close()