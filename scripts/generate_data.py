# generate_data.py
# Generates 1000 realistic orders and saves to data/orders.csv
# This is our source data — same concept as SAP/Salesforce
# feeding real data into your company's Kafka pipeline

import csv
import random
import uuid
from datetime import datetime, timedelta
import os

# Create data folder if it doesn't exist
os.makedirs('data', exist_ok=True)

# Real-looking customer names
customers = [
    "alice_smith", "bob_jones", "carol_white", "dave_brown",
    "eve_davis", "frank_miller", "grace_wilson", "henry_moore",
    "iris_taylor", "jack_anderson", "karen_thomas", "leo_jackson",
    "maya_harris", "noah_martin", "olivia_garcia", "peter_martinez"
]

# Real product categories
categories = [
    "electronics", "clothing", "books", "furniture",
    "sports", "kitchen", "toys", "beauty"
]

# Real order statuses — same ones your company tracks
statuses = ["placed", "shipped", "delivered", "cancelled"]
status_weights = [20, 30, 45, 5]  # delivered is most common

# Generate 1000 orders
orders = []
base_date = datetime(2024, 1, 1)

print("Generating 1000 orders...")

for i in range(1000):
    order_id   = f"ORD-{str(uuid.uuid4())[:8].upper()}"
    customer   = random.choice(customers)
    category   = random.choice(categories)
    amount     = round(random.uniform(9.99, 999.99), 2)
    status     = random.choices(statuses, weights=status_weights)[0]
    order_date = base_date + timedelta(days=random.randint(0, 365))

    orders.append({
        "order_id":   order_id,
        "customer":   customer,
        "category":   category,
        "amount":     amount,
        "status":     status,
        "order_date": order_date.strftime("%Y-%m-%d")
    })

# Write to CSV
output_path = "data/orders.csv"
with open(output_path, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=orders[0].keys())
    writer.writeheader()
    writer.writerows(orders)

print(f"Done. {len(orders)} orders saved to {output_path}")
print(f"Sample row: {orders[0]}")