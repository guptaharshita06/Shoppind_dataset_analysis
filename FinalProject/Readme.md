# Secure Retail Data Lakehouse — Databricks Version
### Celebal Technologies — Data Engineering Assignment

## 📌 Problem Statement
Retail platforms (e-commerce, POS) collect PII (names, addresses, phone, DOB) and PCI data
(card numbers, CVV). Storing this in plain text violates **PCI-DSS**, **GDPR**, and **DPDP**,
and giving analysts unrestricted access to raw identities violates the **principle of least
privilege**. This pipeline masks, tokenizes, encrypts, and access-controls the data through a
**Bronze → Silver → Gold medallion architecture**, ending in a Gold-layer analytics dashboard.

## 📂 Dataset — Nothing to Upload
Like the E-Commerce Analytics project, this notebook **generates its own data** at runtime
(300 customers, 1,000 transactions, via `Faker`) — no external file needed.

## 📥 What to Upload
Just **one file**: `Secure_Retail_Lakehouse_Databricks.py`
- **Databricks:** Workspace → Import → File
- **GitHub:** Push into the `Assignment8` folder (or a new `Assignment9` folder if this is a
  separate submission from the E-Commerce Analytics project — check with your mentor)

## ⬆️ How to Run
1. Import into Databricks (Workspace → Import → File)
2. Attach to a cluster
3. Run the `%pip install faker cryptography` cell first (near the top) — cluster restarts automatically
4. **Run All**
5. Check **Data/Catalog** sidebar → `secure_retail_lakehouse` schema should show 4 tables:
   `bronze_retail_transactions`, `silver_retail_transactions`, `gold_customer_spend`,
   `gold_spend_category_summary`

## 🏗️ Architecture
| Layer | Table(s) | Contains PII/PCI? | Who Accesses |
|-------|----------|---------------------|----------------|
| 🥉 Bronze | `bronze_retail_transactions` | Yes (CVV already hard-dropped) | `compliance_admin` |
| 🥈 Silver | `silver_retail_transactions` | No (masked/tokenized/encrypted) | `data_engineer`, `data_scientist` |
| 🥇 Gold | `gold_customer_spend`, `gold_spend_category_summary` | No | `business_analyst` (everyone) |

## 🔒 Security Controls
1. **Hard-Drop** — `cvv` is dropped from the in-memory data before Bronze is ever written —
   it is never persisted to any table, satisfying PCI-DSS.
2. **Masking** — names, emails, phone numbers partially redacted in Silver.
3. **Tokenization** — card numbers replaced with a salted SHA-256 surrogate token.
4. **Encryption** — the token is further encrypted at rest with Fernet (AES-128); only
   `compliance_admin` (holding the key) can decrypt it.
5. **Feature Engineering** — date of birth → age bucket; transaction amount → spend category
   (`Low <1000`, `Medium 1000-5000`, `High >5000`).
6. **RBAC** — 4 roles (`business_analyst`, `data_scientist`, `data_engineer`,
   `compliance_admin`) each get a different scoped view, demonstrated live in the notebook
   including a denied decrypt attempt and an authorized one.

## 📊 Gold Layer Dashboard
Matches the reference dashboard: Total Spend by Customer (bar), Transaction Distribution by
Spend Category (pie), Total vs Avg Amount by Spend Category (grouped bar), and Avg Spend per
Customer Ranked (line) — built with `matplotlib`, rendered via `plt.show()` in the notebook.

## ✅ Validated Before Delivery
The full pipeline (Bronze ingestion + hard-drop, Silver masking/tokenization/encryption/
feature engineering, Gold aggregation, the dashboard, and the RBAC access/decrypt demo) was
run end-to-end against a local Spark session before delivery — confirmed working.

## 🧰 Tech Stack
PySpark, Delta Lake, `cryptography` (Fernet/AES-128), `hashlib` (SHA-256), `Faker`,
`matplotlib`, Databricks

## ✍️ Author
Harshita — Data Engineering Intern, Celebal Technologies
