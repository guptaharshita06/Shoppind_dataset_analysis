"""
01_generate_raw_data.py
RAW LANDING ZONE — Superstore Dataset Enriched with PII/PCI

The real Kaggle Superstore dataset already contains genuine transactional data
(orders, customers, sales, profit) and one real PII field: Customer Name, along
with City/State/Postal Code. However it does NOT include the other PII fields
(email, phone, date of birth, full street address) or any PCI/payment fields
(card number, CVV) that a real-world retail platform's checkout/POS system
would also capture at the point of transaction.

To build a realistic, complete "raw operational system export" for this
security pipeline, we enrich the real Superstore records with synthetic
PII/PCI fields, deterministically tied to each unique customer/order so the
result behaves like genuine source data (same customer -> same email/phone/DOB
across all their orders; each order -> its own card transaction).
"""

import pandas as pd
import numpy as np
import random
import hashlib
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

SUPERSTORE_PATH = "/mnt/user-data/uploads/Sample_-_Superstore.csv"
RAW_OUTPUT_PATH = "raw/raw_retail_transactions.csv"

card_networks = ["4", "5", "6"]

def deterministic_faker_for_customer(customer_id: str):
    """Seed a per-customer Faker instance so the same customer always gets the
    same synthetic email/phone/DOB/address across every one of their orders —
    exactly how a real customer profile would behave."""
    seed = int(hashlib.md5(customer_id.encode()).hexdigest(), 16) % (2**32)
    local_fake = Faker()
    local_fake.seed_instance(seed)
    return local_fake

def build_raw_dataset(superstore_path: str, output_path: str) -> pd.DataFrame:
    df = pd.read_csv(superstore_path, encoding="latin1")

    # --- Enrich PII per unique customer (consistent across their orders) ---
    customer_profiles = {}
    for cust_id, cust_name in df[["Customer ID", "Customer Name"]].drop_duplicates().values:
        cf = deterministic_faker_for_customer(cust_id)
        customer_profiles[cust_id] = {
            "email": f"{cust_name.lower().replace(' ', '.')}@{cf.free_email_domain()}",
            "phone_number": cf.phone_number(),
            "date_of_birth": cf.date_of_birth(minimum_age=18, maximum_age=75).strftime("%Y-%m-%d"),
        }

    df["email"] = df["Customer ID"].map(lambda c: customer_profiles[c]["email"])
    df["phone_number"] = df["Customer ID"].map(lambda c: customer_profiles[c]["phone_number"])
    df["date_of_birth"] = df["Customer ID"].map(lambda c: customer_profiles[c]["date_of_birth"])

    # --- Enrich PCI per order (each transaction has its own card swipe) ---
    card_numbers, card_expiries, cvvs = [], [], []
    for order_id in df["Order ID"]:
        seed = int(hashlib.md5(order_id.encode()).hexdigest(), 16) % (2**32)
        rnd = random.Random(seed)
        prefix = rnd.choice(card_networks)
        card_numbers.append(prefix + "".join(str(rnd.randint(0, 9)) for _ in range(15)))
        card_expiries.append(f"{rnd.randint(1,12):02d}/{rnd.randint(26,30)}")
        cvvs.append(f"{rnd.randint(100,999)}")

    df["card_number"] = card_numbers
    df["card_expiry"] = card_expiries
    df["cvv"] = cvvs

    # --- Rename to consistent snake_case schema used by the pipeline ---
    df = df.rename(columns={
        "Row ID": "row_id",
        "Order ID": "order_id",
        "Order Date": "order_date",
        "Ship Date": "ship_date",
        "Ship Mode": "ship_mode",
        "Customer ID": "customer_id",
        "Customer Name": "full_name",
        "Segment": "segment",
        "Country": "country",
        "City": "city",
        "State": "state",
        "Postal Code": "postal_code",
        "Region": "region",
        "Product ID": "product_id",
        "Category": "category",
        "Sub-Category": "sub_category",
        "Product Name": "product_name",
        "Sales": "sales",
        "Quantity": "quantity",
        "Discount": "discount",
        "Profit": "profit",
    })

    df.to_csv(output_path, index=False)
    print(f"Raw dataset built from real Superstore data: {df.shape}")
    print(f"Unique customers enriched with PII: {len(customer_profiles)}")
    print(df[["order_id", "full_name", "email", "phone_number", "date_of_birth",
              "card_number", "cvv", "sales"]].head(3).to_string())
    return df

if __name__ == "__main__":
    build_raw_dataset(SUPERSTORE_PATH, RAW_OUTPUT_PATH)
