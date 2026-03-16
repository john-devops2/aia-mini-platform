# producer.py
# Reads real orders from CSV and sends each row as a Kafka message
# This mirrors how your company's source systems feed data into Kafka
# SAP, Salesforce, Workday all act as producers in your real platform

# producer.py
# Reads real orders from CSV and sends each row as a Kafka message
# Writes logs to logs/producer.log so Promtail ships them to Loki

from kafka import KafkaProducer
import json
import csv
import time
import logging
import os

# Set up logging — writes to file AND prints to terminal
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler('logs/producer.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# Connect to Kafka broker
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

log.info("Connected to Kafka broker at localhost:29092")
log.info("Reading orders from data/orders.csv")

sent = 0
with open('data/orders.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        row['amount'] = float(row['amount'])
        producer.send('orders', value=row)
        sent += 1
        if sent % 100 == 0:
            log.info(f"Sent {sent} messages to Kafka topic: orders")
        time.sleep(0.01)

producer.flush()
log.info(f"Producer done. Total sent: {sent} messages")
import json
import csv
import time

# Connect to Kafka broker
# 29092 is our external port — what your laptop uses to reach Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Connected to Kafka broker at localhost:29092")
print("Reading orders from data/orders.csv...\n")

# Read CSV and send each row as a Kafka message
sent = 0
with open('data/orders.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Convert amount to float — CSV reads everything as string
        row['amount'] = float(row['amount'])

        # Send to orders topic
        producer.send('orders', value=row)
        sent += 1

        # Print progress every 100 messages
        if sent % 100 == 0:
            print(f"Sent {sent} messages...")

        # Small delay so Kafka UI shows messages flowing in
        time.sleep(0.01)

# Flush ensures all buffered messages are delivered
producer.flush()
print(f"\nDone. {sent} orders sent to Kafka topic: orders")