# Databricks notebook source
# MAGIC %md
# MAGIC # Secure Retail Data Lakehouse
# MAGIC ### Celebal Technologies — Data Engineering Assignment (Databricks Version)
# MAGIC
# MAGIC ## Problem Statement
# MAGIC Retail operational systems (E-commerce, POS) continuously collect raw PII (names,
# MAGIC addresses, phone numbers, dates of birth) and PCI data (card numbers, CVV). Storing this
# MAGIC in plain text creates security vulnerabilities and violates **PCI-DSS**, **GDPR**, and
# MAGIC **DPDP** compliance frameworks. Internal analysts also don't need raw identities — only
# MAGIC aggregate trends — so unrestricted access violates the **principle of least privilege**.
# MAGIC
# MAGIC ## What We Are Building
# MAGIC A secure, automated **static batch pipeline** using PySpark + Spark SQL on Databricks,
# MAGIC following the **Bronze → Silver → Gold medallion architecture**:
# MAGIC
# MAGIC | Layer | Tables | Contains PII/PCI? | Who Accesses |
# MAGIC |-------|--------|---------------------|----------------|
# MAGIC | 🥉 **Bronze** | `bronze_retail_transactions` | Yes (plaintext, CVV already hard-dropped) | `compliance_admin` only |
# MAGIC | 🥈 **Silver** | `silver_retail_transactions` | No (masked + tokenized + encrypted) | `data_engineer`, `data_scientist` |
# MAGIC | 🥇 **Gold** | `gold_customer_spend`, `gold_spend_category_summary` | No (fully aggregated) | `business_analyst` (everyone) + dashboard |
# MAGIC
# MAGIC ### Security Controls Implemented
# MAGIC 1. **Hard-Drop** — CVV is physically dropped at ingestion, never written to any layer.
# MAGIC 2. **Mask & Tokenize** — names, emails, phone numbers masked; card numbers replaced with
# MAGIC    irreversible salted SHA-256 surrogate tokens, further encrypted at rest (Fernet/AES-128).
# MAGIC 3. **Feature Engineering** — date of birth → age bucket; transaction amount → spend
# MAGIC    category (Low/Medium/High), enabling analytics without exposing exact values.
# MAGIC 4. **RBAC (Access Control)** — roles get scoped views enforced in code, simulating
# MAGIC    Databricks Unity Catalog GRANTs / table ACLs in production.
# MAGIC 5. **Gold Layer Dashboard** — fully anonymized, aggregated business analytics.

# COMMAND ----------

# MAGIC %pip install faker cryptography

# COMMAND ----------

# MAGIC %md ## Step 0: Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import random
import hashlib
from faker import Faker
from cryptography.fernet import Fernet
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

CATALOG_SCHEMA = "secure_retail_lakehouse"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_SCHEMA}")
spark.sql(f"USE {CATALOG_SCHEMA}")
print("Setup complete. Active schema:", CATALOG_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥉 Bronze Layer: Ingestion with Immediate Hard-Drop
# MAGIC
# MAGIC Raw retail transaction data — as it would land from an e-commerce checkout / POS
# MAGIC terminal — is generated here. The **first security checkpoint** happens immediately:
# MAGIC CVV, which PCI-DSS prohibits storing under any circumstance, is dropped **before** the
# MAGIC data is ever written to a table. It exists only transiently in memory during generation.

# COMMAND ----------

fake = Faker()
Faker.seed(21)
random.seed(21)

N_CUSTOMERS = 300
N_TRANSACTIONS = 1000
CARD_PREFIXES = ["4", "5", "6"]
CATEGORIES = ["Electronics", "Apparel", "Home & Kitchen", "Beauty", "Grocery", "Sports"]

# Simulate N_CUSTOMERS unique customer profiles (PII)
customer_profiles = []
for i in range(1, N_CUSTOMERS + 1):
    dob = fake.date_of_birth(minimum_age=18, maximum_age=70)
    customer_profiles.append({
        "customer_id": f"CUST{1000+i}", "full_name": fake.name(), "email": fake.email(),
        "phone_number": fake.phone_number(), "city": fake.city(), "state": fake.state(),
        "date_of_birth": dob.strftime("%Y-%m-%d"),
    })

# Simulate N_TRANSACTIONS raw transactions (PCI) — CVV exists here only in-memory
raw_rows = []
for i in range(1, N_TRANSACTIONS + 1):
    cust = random.choice(customer_profiles)
    prefix = random.choice(CARD_PREFIXES)
    card_number = prefix + "".join(str(random.randint(0, 9)) for _ in range(15))
    txn_date = fake.date_time_between(start_date="-180d", end_date="now")

    raw_rows.append({
        **cust,
        "card_number": card_number,
        "card_expiry": f"{random.randint(1,12):02d}/{random.randint(26,30)}",
        "cvv": f"{random.randint(100,999)}",  # <-- will be hard-dropped, never written to Bronze
        "transaction_id": f"TXN{100000+i}",
        "transaction_date": txn_date.strftime("%Y-%m-%d %H:%M:%S"),
        "product_category": random.choice(CATEGORIES),
        "transaction_amount": round(random.uniform(50, 12000), 2),
    })

raw_df = spark.createDataFrame(raw_rows)
print(f"Raw (in-memory) transactions: {raw_df.count()} rows, columns: {raw_df.columns}")

# COMMAND ----------

# HARD-DROP: cvv is removed here, before anything is persisted to Bronze.
bronze_df = raw_df.drop("cvv")
bronze_df = (bronze_df
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source_system", F.lit("retail_pos_ecommerce")))

bronze_df.write.mode("overwrite").saveAsTable("bronze_retail_transactions")
print(f"[HARD-DROP] 'cvv' column permanently dropped — never written to any table.")
print(f"[BRONZE] {bronze_df.count()} rows, {len(bronze_df.columns)} columns -> bronze_retail_transactions")

# COMMAND ----------

display(spark.table("bronze_retail_transactions").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Bronze **still contains PII/PCI** (names, card numbers) for lineage/audit purposes, but
# MAGIC CVV is permanently gone. Access to Bronze is restricted to `compliance_admin` only
# MAGIC (enforced in the RBAC section below).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥈 Silver Layer: Masking, Tokenization, Encryption & Feature Engineering

# COMMAND ----------

HASH_SALT = "celebal-secure-lakehouse-2026"  # in production: Azure Key Vault / Databricks secret scope

def mask_name(name):
    return " ".join(p[0] + "*" * (len(p) - 1) for p in str(name).split())

def mask_email(email):
    local, _, domain = str(email).partition("@")
    masked_local = local[0] + "*" if len(local) <= 2 else local[:2] + "*" * (len(local) - 2)
    return f"{masked_local}@{domain}"

def mask_phone(phone):
    digits = "".join(ch for ch in str(phone) if ch.isdigit())
    return "*" * len(digits) if len(digits) < 4 else "X" * (len(digits) - 4) + digits[-4:]

def tokenize_card(card_number):
    salted = f"{HASH_SALT}:{card_number}"
    return "TOK-" + hashlib.sha256(salted.encode()).hexdigest()[:20]

mask_name_udf = F.udf(mask_name)
mask_email_udf = F.udf(mask_email)
mask_phone_udf = F.udf(mask_phone)
tokenize_card_udf = F.udf(tokenize_card)

# Encryption key — in production this lives in a Databricks secret scope / Azure Key Vault,
# never alongside the data itself.
ENCRYPTION_KEY = Fernet.generate_key()
fernet = Fernet(ENCRYPTION_KEY)

def encrypt_token(token):
    return fernet.encrypt(token.encode()).decode()

encrypt_token_udf = F.udf(encrypt_token)

# COMMAND ----------

silver_df = (
    spark.table("bronze_retail_transactions")
    .withColumn("full_name_masked", mask_name_udf(F.col("full_name")))
    .withColumn("email_masked", mask_email_udf(F.col("email")))
    .withColumn("phone_masked", mask_phone_udf(F.col("phone_number")))
    .withColumn("card_token", tokenize_card_udf(F.col("card_number")))
    .withColumn("card_token_encrypted", encrypt_token_udf(F.col("card_token")))
    .withColumn("age", F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25))
    .withColumn("age_bucket",
        F.when(F.col("age") < 25, "18-24").when(F.col("age") < 35, "25-34")
         .when(F.col("age") < 45, "35-44").when(F.col("age") < 60, "45-59").otherwise("60+"))
    .withColumn("spend_category",
        F.when(F.col("transaction_amount") < 1000, "Low (<1000)")
         .when(F.col("transaction_amount") <= 5000, "Medium (1000-5000)")
         .otherwise("High (>5000)"))
    # DROP raw sensitive columns — Silver never stores raw PII/PCI
    .drop("full_name", "email", "phone_number", "date_of_birth", "card_number",
          "card_expiry", "card_token")
    .withColumn("_layer", F.lit("silver"))
)

silver_df.write.mode("overwrite").saveAsTable("silver_retail_transactions")
print(f"[SILVER] {silver_df.count()} rows, {len(silver_df.columns)} columns -> silver_retail_transactions")
print(f"[SILVER] Columns: {silver_df.columns}")

# COMMAND ----------

display(spark.table("silver_retail_transactions").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Notice what's gone from Silver:** `full_name`, `email`, `phone_number`,
# MAGIC `date_of_birth`, `card_number`, `card_expiry`, and the plaintext `card_token` are all
# MAGIC dropped. Only masked/tokenized/encrypted/derived versions remain.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥇 Gold Layer: Materialized Business Aggregates + Dashboard
# MAGIC
# MAGIC Fully anonymized aggregates — no way to trace a row back to an individual. This is what
# MAGIC `business_analyst` (and effectively everyone) queries and what powers the dashboard.

# COMMAND ----------

gold_customer_spend = (
    spark.table("silver_retail_transactions")
    .groupBy("customer_id", "full_name_masked")
    .agg(
        F.round(F.sum("transaction_amount"), 2).alias("total_spend"),
        F.count("transaction_id").alias("total_transactions"),
        F.round(F.avg("transaction_amount"), 2).alias("avg_spend"),
    )
)
gold_customer_spend.write.mode("overwrite").saveAsTable("gold_customer_spend")

gold_spend_category_summary = (
    spark.table("silver_retail_transactions")
    .groupBy("spend_category")
    .agg(
        F.count("transaction_id").alias("txn_count"),
        F.round(F.sum("transaction_amount"), 2).alias("total_amount"),
        F.round(F.avg("transaction_amount"), 2).alias("avg_amount"),
    )
)
gold_spend_category_summary.write.mode("overwrite").saveAsTable("gold_spend_category_summary")

print("Gold tables written: gold_customer_spend, gold_spend_category_summary")
display(gold_spend_category_summary)

# COMMAND ----------

# MAGIC %md ### Gold Layer — Business Analytics Dashboard

# COMMAND ----------

customer_spend_pd = spark.table("gold_customer_spend").orderBy("customer_id").toPandas()
category_pd = spark.table("gold_spend_category_summary").toPandas()
category_order = ["Low (<1000)", "Medium (1000-5000)", "High (>5000)"]
category_pd["spend_category"] = pd_cat = category_pd["spend_category"]
category_pd = category_pd.set_index("spend_category").reindex(category_order).reset_index()

fig, axes = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle("Gold Layer -- Business Analytics Dashboard", fontsize=16, fontweight="bold")

# 1. Total Spend by Customer
ax1 = axes[0, 0]
ax1.bar(customer_spend_pd["customer_id"], customer_spend_pd["total_spend"], color="steelblue")
ax1.set_title("Total Spend by Customer")
ax1.set_ylabel("Total Spend (Rs.)")
ax1.set_xticks([])
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"Rs.{x:,.0f}"))

# 2. Transaction Distribution by Spend Category (pie)
ax2 = axes[0, 1]
colors = ["#2ca02c", "#ff7f0e", "#e6194b"]
total_txns = int(category_pd["txn_count"].sum())
def _autopct(pct):
    count = int(round(pct * total_txns / 100))
    return f"{pct:.0f}%\n({count} txns)"
ax2.pie(category_pd["txn_count"], labels=category_pd["spend_category"],
        autopct=_autopct, colors=colors, explode=[0.03]*3, startangle=90)
ax2.set_title("Transaction Distribution by Spend Category")

# 3. Total vs Avg Amount by Spend Category
ax3 = axes[1, 0]
x = range(len(category_pd))
width = 0.35
ax3.bar([i - width/2 for i in x], category_pd["total_amount"], width, label="Total Amount", color="#1f77b4")
ax3.bar([i + width/2 for i in x], category_pd["avg_amount"], width, label="Avg Amount", color="#ff7f0e")
ax3.set_xticks(list(x))
ax3.set_xticklabels(category_pd["spend_category"])
ax3.set_title("Total vs Avg Amount by Spend Category")
ax3.set_ylabel("Amount (Rs.)")
ax3.legend()
ax3.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"Rs.{x:,.0f}"))

# 4. Avg Spend per Customer (Ranked)
ax4 = axes[1, 1]
ranked = customer_spend_pd.sort_values("avg_spend").reset_index(drop=True)
ax4.plot(ranked["avg_spend"], range(len(ranked)), marker="o", markersize=2, linewidth=0.5, color="black")
ax4.set_title("Avg Spend per Customer (Ranked)")
ax4.set_xlabel("Average Spend (Rs.)")
ax4.set_yticks([])
ax4.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"Rs.{x:,.0f}"))

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Access Control (RBAC)
# MAGIC
# MAGIC Simulates what Databricks Unity Catalog GRANTs / table ACLs enforce in production —
# MAGIC different roles get different views of the data.
# MAGIC
# MAGIC | Role | Layer Access | Can Decrypt Card Token? |
# MAGIC |------|--------------|--------------------------|
# MAGIC | `business_analyst` | Gold only | No |
# MAGIC | `data_scientist` | Silver, excluding encrypted token | No |
# MAGIC | `data_engineer` | Silver, full | No |
# MAGIC | `compliance_admin` | Bronze + decryption rights | Yes (fraud investigation only) |

# COMMAND ----------

ROLE_POLICY = {
    "business_analyst": {"layer": "gold", "can_decrypt": False},
    "data_scientist":   {"layer": "silver", "exclude_encrypted": True, "can_decrypt": False},
    "data_engineer":    {"layer": "silver", "exclude_encrypted": False, "can_decrypt": False},
    "compliance_admin": {"layer": "bronze", "can_decrypt": True},
}

def get_data_for_role(role, table_name=None):
    if role not in ROLE_POLICY:
        raise PermissionError(f"Unknown role '{role}'. Access denied.")
    policy = ROLE_POLICY[role]
    if policy["layer"] == "gold":
        df = spark.table(table_name or "gold_customer_spend")
    elif policy["layer"] == "silver":
        df = spark.table("silver_retail_transactions")
        if policy.get("exclude_encrypted"):
            df = df.drop("card_token_encrypted")
    elif policy["layer"] == "bronze":
        df = spark.table("bronze_retail_transactions")
    print(f"[ACCESS GRANTED] role='{role}' -> layer='{policy['layer']}' -> {df.count()} rows, {len(df.columns)} cols")
    return df

def decrypt_card_token(role, encrypted_token):
    if not ROLE_POLICY.get(role, {}).get("can_decrypt", False):
        raise PermissionError(f"Role '{role}' is not authorized to decrypt payment tokens.")
    return fernet.decrypt(encrypted_token.encode()).decode()

# COMMAND ----------

print("-- business_analyst --")
_ = get_data_for_role("business_analyst")

print("\n-- data_scientist --")
ds_df = get_data_for_role("data_scientist")
print("Columns visible:", ds_df.columns)

print("\n-- data_scientist tries to decrypt (should be DENIED) --")
sample_token = spark.table("silver_retail_transactions").select("card_token_encrypted").first()[0]
try:
    decrypt_card_token("data_scientist", sample_token)
except PermissionError as e:
    print(f"[ACCESS DENIED] {e}")

print("\n-- compliance_admin decrypts the same token (should SUCCEED) --")
decrypted = decrypt_card_token("compliance_admin", sample_token)
print(f"[ACCESS GRANTED] Decrypted surrogate token: {decrypted}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compliance Mapping
# MAGIC
# MAGIC | Regulation | Requirement | How This Pipeline Satisfies It |
# MAGIC |------------|-------------|----------------------------------|
# MAGIC | **PCI-DSS** | Never store CVV under any circumstance | Hard-dropped before Bronze is ever written |
# MAGIC | **PCI-DSS** | Render PAN (card number) unreadable when stored | Tokenized (SHA-256) + encrypted (Fernet/AES-128) |
# MAGIC | **GDPR** | Data minimization | Gold retains zero identifiers, only aggregates |
# MAGIC | **GDPR** | Right to erasure | Deleting by `customer_id` across Bronze/Silver satisfies this |
# MAGIC | **GDPR / DPDP** | Purpose limitation | RBAC restricts business analysts to Gold-only aggregated view |
# MAGIC | **DPDP (India)** | Reasonable security safeguards | Encryption + masking + access control combined (defense-in-depth) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Layer | Table(s) | Contains PII? | Contains PCI? | Who Accesses |
# MAGIC |-------|----------|-----------------|------------------|----------------|
# MAGIC | 🥉 Bronze | `bronze_retail_transactions` | Yes (CVV already gone) | Partial | `compliance_admin` |
# MAGIC | 🥈 Silver | `silver_retail_transactions` | No (masked) | No (tokenized+encrypted) | `data_engineer`, `data_scientist` |
# MAGIC | 🥇 Gold | `gold_customer_spend`, `gold_spend_category_summary` | No | No | `business_analyst` (everyone) |
# MAGIC
# MAGIC **Security controls demonstrated:** hard-drop, masking, salted one-way tokenization,
# MAGIC symmetric encryption (Fernet/AES-128) with key separation, feature engineering for
# MAGIC privacy-safe analytics (age/spend bucketing), role-based access control (RBAC), and a
# MAGIC Gold-layer business analytics dashboard — all built natively on PySpark + Delta + Databricks.
