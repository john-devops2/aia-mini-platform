# consumer.py
# Reads from Kafka and writes to PostgreSQL
# Writes logs to logs/consumer.log so Promtail ships them to Loki

from kafka import KafkaConsumer
import psycopg2
import json
import logging
import os

# Set up logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler('logs/consumer.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost", port=5432,
    database="aiadb", user="aiauser", password="aiapass"
)
cursor = conn.cursor()
log.info("Connected to PostgreSQL")

# Create table
cursor.execute("DROP TABLE IF EXISTS orders")
cursor.execute("""
    CREATE TABLE orders (
        order_id    TEXT PRIMARY KEY,
        customer    TEXT,
        category    TEXT,
        amount      NUMERIC,
        status      TEXT,
        order_date  DATE,
        consumed_at TIMESTAMP DEFAULT NOW()
    )
""")
conn.commit()
log.info("Table orders ready")

# Connect to Kafka
consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:29092',
    group_id='orders-consumer-group-v3',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    consumer_timeout_ms=15000
)
log.info("Connected to Kafka. Reading messages")

count = 0
for message in consumer:
    order = message.value
    cursor.execute("""
        INSERT INTO orders (order_id, customer, category, amount, status, order_date)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO NOTHING
    """, (
        order['order_id'], order['customer'], order['category'],
        float(order['amount']), order['status'], order['order_date']
    ))
    conn.commit()
    count += 1
    if count % 100 == 0:
        log.info(f"Consumed {count} messages | partition {message.partition} offset {message.offset}")

log.info(f"Consumer done. Total written to PostgreSQL: {count}")
cursor.close()
conn.close()