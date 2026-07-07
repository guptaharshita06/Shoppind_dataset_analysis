# Secure Retail Data Lakehouse
### Celebal Technologies — Data Engineering Assignment

## 📌 Problem Statement
Retail platforms (e-commerce, POS) continuously collect PII (names, addresses, phone numbers,
DOB) and PCI data (card numbers, CVV). Storing this in plain text creates security risks and
violates **PCI-DSS**, **GDPR**, and **DPDP** compliance frameworks. Analysts need aggregate
trends, not raw customer identities — so unrestricted internal access also violates the
**principle of least privilege**.

## 🏗️ Solution: Bronze → Silver → Gold Lakehouse

```
raw/            → unprocessed source data (simulates operational system export)
     ↓  [01_generate_raw_data.py]
bronze/         → CVV hard-dropped at ingestion; still has masked-later PII (audit/lineage)
     ↓  [02_bronze_ingestion.py]
silver/         → PII masked, PCI tokenized + encrypted, features engineered
     ↓  [03_silver_transformation.py]
gold/           → fully aggregated, zero identifiers — safe for open analyst access
     ↓  [04_gold_aggregation.py]
keys/           → encryption key (simulates Azure Key Vault secret storage)
[05_rbac_access_control.py]  → role-based access simulation across all layers
```

## 📂 Files in this Submission
| File | Purpose |
|------|---------|
| `Secure_Retail_Lakehouse.ipynb` | **Main deliverable** — full pipeline with explanations & executed outputs |
| `01_generate_raw_data.py` | Generates synthetic raw retail data (PII + PCI fields) |
| `02_bronze_ingestion.py` | Bronze layer — hard-drops CVV at ingestion |
| `03_silver_transformation.py` | Silver layer — masking, tokenization, encryption, feature engineering |
| `04_gold_aggregation.py` | Gold layer — fully anonymized aggregates for analytics |
| `05_rbac_access_control.py` | Role-based access control simulation |
| `raw/`, `bronze/`, `silver/`, `gold/`, `keys/` | The actual lakehouse data layers produced by running the scripts |

## 🔒 Security Controls Implemented
1. **Hard-Drop** — CVV is deleted permanently at ingestion (PCI-DSS: never store CVV, ever).
2. **Masking** — names, emails, phone numbers partially redacted (e.g. `garzaanthony@example.org` → `ga**********@example.org`).
3. **Tokenization** — card numbers replaced with a salted SHA-256 surrogate token (irreversible, but consistent for joins).
4. **Encryption** — the token is further encrypted at rest with Fernet (AES-128); only a role holding the key can decrypt.
5. **Feature Engineering** — date of birth → age → age bucket; transaction amount → amount bucket (enables analysis without exposing exact values).
6. **RBAC** — 4 roles (`business_analyst`, `data_scientist`, `data_engineer`, `compliance_admin`) each get a different scoped view, simulating Azure Data Lake ACLs / Databricks Unity Catalog GRANTs.

## 📋 Compliance Mapping
| Regulation | Requirement | How It's Satisfied |
|------------|-------------|----------------------|
| PCI-DSS | Never store CVV | Hard-dropped at Bronze ingestion |
| PCI-DSS | Render PAN unreadable at rest | Tokenized + encrypted |
| GDPR | Data minimization | Gold has zero identifiers |
| GDPR | Right to erasure | Deleting by `customer_id` across layers satisfies this |
| GDPR / DPDP | Purpose limitation | RBAC restricts analysts to aggregated Gold only |
| DPDP | Reasonable security safeguards | Encryption + masking + access control (defense-in-depth) |

## ▶️ How to Run
```bash
pip install pandas numpy faker cryptography pyarrow
python 01_generate_raw_data.py
python 02_bronze_ingestion.py
python 03_silver_transformation.py
python 04_gold_aggregation.py
python 05_rbac_access_control.py
```
Or simply open and run `Secure_Retail_Lakehouse.ipynb` end-to-end — it walks through every
layer and the RBAC demo with explanations.

## 🧰 Tech Stack
- Python 3, Pandas, NumPy
- `cryptography` (Fernet/AES-128) for encryption
- `hashlib` (SHA-256) for tokenization
- `Faker` for realistic synthetic PII generation
- Parquet (via PyArrow) for Bronze/Silver storage — CSV for Gold (analyst-friendly)

## ⚠️ Note on Dataset
This project uses the **real Superstore Sales Dataset** (Kaggle, 9,994 orders / 793 unique
customers) as the transactional backbone — the same dataset used in the Pandas cleaning
assignment. Since Superstore doesn't include payment/PCI fields (card number, CVV) or the
full PII set (email, phone, date of birth) that a real e-commerce/POS checkout would also
capture, these are enriched **deterministically** per customer/order in
`01_generate_raw_data.py`:
- Each customer gets the *same* synthetic email/phone/DOB across all their orders (consistent
  customer profile, not random per row).
- Each order gets its *own* synthetic card number/CVV (a real transaction has one card swipe
  per purchase).

`Customer Name`, `City`, `State`, `Sales`, `Profit`, etc. are all genuine Superstore values —
only the PII/PCI fields that Superstore doesn't provide were synthetically added, so the
security pipeline (masking/tokenization/encryption/RBAC) operates on realistic, consistent
data end-to-end.

## ✍️ Author
Harshita — Data Engineering Intern, Celebal Technologies
