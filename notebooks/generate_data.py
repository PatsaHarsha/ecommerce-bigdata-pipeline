"""
generate_data.py — Run this locally (outside Docker) to produce data/raw/orders.csv
Requires: pip install faker pandas
"""
import os, random
import pandas as pd
from faker import Faker

fake = Faker("en_US")
Faker.seed(42)
random.seed(42)

N = 55000
STATUSES   = ["pending","shipped","cancelled","delivered","returned"]
BAD_STATUS = ["unknown","error","null"]
CATEGORIES = ["Electronics","Clothing","Books","Home & Garden","Sports",
               "Toys","Beauty","Automotive","Food","Jewelry"]
PAYMENTS   = ["credit_card","debit_card","paypal","bank_transfer","crypto"]
STATES     = ["CA","TX","FL","NY","PA","IL","OH","GA","NC","MI",
               "NJ","VA","WA","AZ","MA","TN","IN","MO","MD","WI",
               "CO","MN","SC","AL","LA","KY","OR","OK","CT","UT"]

records = []
for i in range(N):
    amount = round(random.uniform(5, 5000), 2)
    if random.random() < 0.01:
        amount = round(random.uniform(-500, -0.01), 2)        # DQ1 violation

    if random.random() < 0.005:
        order_date = fake.date_between(start_date="today", end_date="+2y")   # DQ2
    else:
        order_date = fake.date_between(start_date="2022-01-01", end_date="2025-12-31")

    status = random.choice(BAD_STATUS) if random.random() < 0.005 else random.choice(STATUSES)  # DQ3

    records.append({
        "order_id":       f"ORD-{100000+i}",
        "customer_id":    f"CUST-{random.randint(1000,9999)}",
        "customer_name":  fake.name(),
        "email":          fake.email(),
        "product_id":     f"PROD-{random.randint(100,999)}",
        "product_name":   fake.catch_phrase(),
        "category":       random.choice(CATEGORIES),
        "order_amount":   amount,
        "quantity":       random.randint(1,10),
        "status":         status,
        "order_date":     str(order_date),
        "shipping_state": random.choice(STATES),
        "payment_method": random.choice(PAYMENTS),
    })

out = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "orders.csv")
os.makedirs(os.path.dirname(out), exist_ok=True)
df = pd.DataFrame(records)
df.to_csv(out, index=False)
print(f"Saved {len(df):,} rows → {out}")
