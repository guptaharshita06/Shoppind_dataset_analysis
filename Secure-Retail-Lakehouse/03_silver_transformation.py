"""
03_silver_transformation.py
SILVER LAYER — Masking, Tokenization, Encryption & Feature Engineering

Purpose:
  Transforms Bronze data into a security-hardened, analytics-safe Silver
  layer. Direct identifiers are masked or tokenized so they cannot be
  reverse-engineered. This is the layer data engineers / trusted internal
  tools may query — but NOT the layer general analysts see (they get Gold).

Security controls applied:
  1. MASKING          -> partial redaction of names, emails, phone, address
  2. TOKENIZATION      -> irreversible SHA-256 hashing of card numbers
                          (salted, so the token cannot be reversed to the
                          original card number, but the same card always
                          maps to the same token — enabling joins/analytics
                          without exposing the real number)
  3. ENCRYPTION        -> the surrogate card token is additionally encrypted
                          at rest (Fernet/AES-128) as a defense-in-depth
                          measure; only holders of the key (stored separately
                          in keys/, simulating a Key Vault) can decrypt it
  4. FEATURE ENGINEERING -> date_of_birth -> age -> age_bucket (drops raw DOB)
                            transaction_amount -> amount_bucket
"""

import pandas as pd
import hashlib
import os
from datetime import datetime
from cryptography.fernet import Fernet

BRONZE_PATH = "bronze/retail_bronze.parquet"
SILVER_PATH = "silver/retail_silver.parquet"
KEY_PATH = "keys/encryption.key"

# Secret salt for hashing (in production: stored in a secrets manager / Azure Key Vault,
# NEVER hardcoded in source control)
HASH_SALT = "celebal-retail-lakehouse-2026"


# ---------- Masking helpers ----------

def mask_name(name: str) -> str:
    """'Daniel Doyle' -> 'D**** D***'"""
    parts = str(name).split()
    return " ".join(p[0] + "*" * (len(p) - 1) for p in parts)

def mask_email(email: str) -> str:
    """'garzaanthony@example.org' -> 'ga**********@example.org'"""
    local, _, domain = str(email).partition("@")
    if len(local) <= 2:
        masked_local = local[0] + "*"
    else:
        masked_local = local[:2] + "*" * (len(local) - 2)
    return f"{masked_local}@{domain}"

def mask_phone(phone: str) -> str:
    """Keep only last 4 digits visible."""
    digits = "".join(ch for ch in str(phone) if ch.isdigit())
    if len(digits) < 4:
        return "*" * len(digits)
    return "X" * (len(digits) - 4) + digits[-4:]

def tokenize_card(card_number: str) -> str:
    """One-way salted hash -> irreversible surrogate token."""
    salted = f"{HASH_SALT}:{card_number}"
    return "TOK-" + hashlib.sha256(salted.encode()).hexdigest()[:20]

def mask_card_last4(card_number: str) -> str:
    card_number = str(card_number)
    return "X" * (len(card_number) - 4) + card_number[-4:]

def get_or_create_key(key_path: str) -> bytes:
    if os.path.exists(key_path):
        with open(key_path, "rb") as f:
            return f.read()
    key = Fernet.generate_key()
    os.makedirs(os.path.dirname(key_path), exist_ok=True)
    with open(key_path, "wb") as f:
        f.write(key)
    print(f"[ENCRYPTION] New encryption key generated and stored at {key_path} "
          f"(simulates an Azure Key Vault secret — never stored alongside data).")
    return key

def age_bucket(age: int) -> str:
    if age < 25:
        return "18-24"
    elif age < 35:
        return "25-34"
    elif age < 45:
        return "35-44"
    elif age < 60:
        return "45-59"
    return "60+"

def amount_bucket(amount: float) -> str:
    if amount < 50:
        return "Low (<50)"
    elif amount < 500:
        return "Medium (50-500)"
    elif amount < 1500:
        return "High (500-1500)"
    return "Very High (1500+)"


def transform_to_silver(bronze_path: str, silver_path: str, key_path: str) -> pd.DataFrame:
    df = pd.read_parquet(bronze_path)
    key = get_or_create_key(key_path)
    fernet = Fernet(key)

    # --- MASKING: direct identifiers ---
    df["full_name_masked"] = df["full_name"].apply(mask_name)
    df["email_masked"] = df["email"].apply(mask_email)
    df["phone_masked"] = df["phone_number"].apply(mask_phone)
    df["card_number_masked"] = df["card_number"].apply(mask_card_last4)

    # --- TOKENIZATION: irreversible surrogate for joins/analytics ---
    df["card_token"] = df["card_number"].apply(tokenize_card)

    # --- ENCRYPTION: encrypt the token again for defense-in-depth at rest ---
    df["card_token_encrypted"] = df["card_token"].apply(
        lambda t: fernet.encrypt(t.encode()).decode()
    )

    # --- FEATURE ENGINEERING ---
    df["date_of_birth"] = pd.to_datetime(df["date_of_birth"])
    today = pd.Timestamp(datetime.utcnow().date())
    df["age"] = ((today - df["date_of_birth"]).dt.days // 365).astype(int)
    df["age_bucket"] = df["age"].apply(age_bucket)
    df["amount_bucket"] = df["sales"].apply(amount_bucket)

    # --- DROP raw sensitive columns entirely from Silver ---
    # Silver keeps masked/tokenized/derived versions ONLY — never the raw values.
    sensitive_cols_to_drop = [
        "full_name", "email", "phone_number",
        "date_of_birth", "card_number", "card_expiry", "card_token",  # keep only encrypted token
        "postal_code",
    ]
    df_silver = df.drop(columns=[c for c in sensitive_cols_to_drop if c in df.columns])
    df_silver["_layer"] = "silver"

    df_silver.to_parquet(silver_path, index=False)
    print(f"[SILVER] Transformed {df_silver.shape[0]} rows, {df_silver.shape[1]} columns -> {silver_path}")
    print(f"[SILVER] Columns: {list(df_silver.columns)}")
    return df_silver

if __name__ == "__main__":
    transform_to_silver(BRONZE_PATH, SILVER_PATH, KEY_PATH)
