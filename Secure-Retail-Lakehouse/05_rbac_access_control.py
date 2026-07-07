"""
05_rbac_access_control.py
ACCESS CONTROL LAYER — Role-Based Access Control (RBAC)

Purpose:
  Simulates what would be enforced via Azure Data Lake ACLs / Databricks
  Unity Catalog / SQL GRANT statements in production. Different roles get
  different views of the data, enforced in code here for demonstration.

Roles:
  - business_analyst : Gold layer ONLY (fully aggregated, zero identifiers)
  - data_scientist    : Silver layer, EXCLUDING the encrypted card token
                        (needs behavioral features, not raw payment data)
  - data_engineer     : Silver layer, full access (needs to validate pipeline)
                        but still no Bronze/raw PII — engineers debug the
                        pipeline logic, not browse customer identities
  - compliance_admin   : Bronze layer + ability to decrypt Silver's encrypted
                        token (only role holding the encryption key) — used
                        for fraud investigation / legal holds only, fully
                        audit-logged in production
"""

import pandas as pd
from cryptography.fernet import Fernet

BRONZE_PATH = "bronze/retail_bronze.parquet"
SILVER_PATH = "silver/retail_silver.parquet"
GOLD_PATH = "gold/retail_gold_aggregated.csv"
KEY_PATH = "keys/encryption.key"

ROLE_POLICY = {
    "business_analyst": {"layer": "gold", "columns": "all", "can_decrypt": False},
    "data_scientist": {"layer": "silver", "columns": "exclude_encrypted", "can_decrypt": False},
    "data_engineer": {"layer": "silver", "columns": "all", "can_decrypt": False},
    "compliance_admin": {"layer": "bronze", "columns": "all", "can_decrypt": True},
}

def get_data_for_role(role: str) -> pd.DataFrame:
    if role not in ROLE_POLICY:
        raise PermissionError(f"Unknown role '{role}'. Access denied.")

    policy = ROLE_POLICY[role]
    layer = policy["layer"]

    if layer == "gold":
        df = pd.read_csv(GOLD_PATH)
    elif layer == "silver":
        df = pd.read_parquet(SILVER_PATH)
        if policy["columns"] == "exclude_encrypted":
            df = df.drop(columns=["card_token_encrypted"])
    elif layer == "bronze":
        df = pd.read_parquet(BRONZE_PATH)
    else:
        raise PermissionError("Invalid layer in policy.")

    print(f"[ACCESS GRANTED] role='{role}' -> layer='{layer}' -> {df.shape[0]} rows, {df.shape[1]} cols")
    return df

def decrypt_card_token(role: str, encrypted_token: str) -> str:
    """Only compliance_admin can decrypt — everyone else is denied."""
    if not ROLE_POLICY.get(role, {}).get("can_decrypt", False):
        raise PermissionError(f"Role '{role}' is not authorized to decrypt payment tokens.")
    with open(KEY_PATH, "rb") as f:
        key = f.read()
    fernet = Fernet(key)
    return fernet.decrypt(encrypted_token.encode()).decode()


if __name__ == "__main__":
    print("=== RBAC Demonstration ===\n")

    print("-- business_analyst tries to access data --")
    analyst_df = get_data_for_role("business_analyst")
    print(analyst_df.head(2), "\n")

    print("-- data_scientist tries to access data --")
    ds_df = get_data_for_role("data_scientist")
    print(f"Columns visible: {list(ds_df.columns)}\n")

    print("-- data_scientist tries to decrypt a card token (should be DENIED) --")
    try:
        sample_token = pd.read_parquet(SILVER_PATH)["card_token_encrypted"].iloc[0]
        decrypt_card_token("data_scientist", sample_token)
    except PermissionError as e:
        print(f"[ACCESS DENIED] {e}\n")

    print("-- compliance_admin decrypts the same token (should SUCCEED, for fraud investigation only) --")
    decrypted = decrypt_card_token("compliance_admin", sample_token)
    print(f"[ACCESS GRANTED] Decrypted token: {decrypted}\n")

    print("-- unknown role tries to access data (should be DENIED) --")
    try:
        get_data_for_role("random_intern")
    except PermissionError as e:
        print(f"[ACCESS DENIED] {e}")
