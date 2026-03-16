# consumer.py
# Reads messages from Kafka orders topic
# Writes each message into PostgreSQL
# Handles the real CSV data shape with category and order_date fields

from kafka import KafkaConsumer
import psycopg2
import json

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="aiadb",
    user="aiauser",
    password="aiapass"
)
cursor = conn.cursor()
print("Connected to PostgreSQL")

# Drop old table and create fresh one with all real fields
# We drop because old table has fewer columns than new data
cursor.execute("DROP TABLE IF EXISTS orders")
cursor.execute("""
    CREATE TABLE orders (
        order_id   TEXT PRIMARY KEY,
        customer   TEXT,
        category   TEXT,
        amount     NUMERIC,
        status     TEXT,
        order_date DATE,
        consumed_at TIMESTAMP DEFAULT NOW()
    )
""")
conn.commit()
print("Table 'orders' created with real schema\n")

# Connect to Kafka
# group_id tracks progress — if restarted it continues from last offset
# auto_offset_reset='earliest' means start from first message
consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:29092',
    group_id='orders-consumer-group-v2',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    consumer_timeout_ms=15000
)
print("Connected to Kafka. Reading messages...\n")

# Read and store each message
count = 0
for message in consumer:
    order = message.value

    cursor.execute("""
        INSERT INTO orders (order_id, customer, category, amount, status, order_date)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO NOTHING
    """, (
        order['order_id'],
        order['customer'],
        order['category'],
        float(order['amount']),
        order['status'],
        order['order_date']
    ))
    conn.commit()
    count += 1

    # Print progress every 100 messages
    if count % 100 == 0:
        print(f"Consumed {count} messages | partition: {message.partition} | offset: {message.offset}")

print(f"\nDone. {count} messages written to PostgreSQL")
cursor.close()
conn.close()