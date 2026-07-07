"""
02_bronze_ingestion.py
BRONZE LAYER — Raw Ingestion with Immediate Hard-Drop

Purpose:
  Ingests raw retail data from the landing zone (raw/) into the Bronze layer.
  This is the FIRST security checkpoint: highly sensitive fields that should
  NEVER be persisted anywhere in the lakehouse (like CVV) are hard-dropped
  immediately during ingestion, before the data is written to disk.

  Bronze still contains PII/PCI (names, card numbers, addresses) in near-raw
  form because it preserves data lineage/auditability — but it is NOT
  analytics-accessible. Access to Bronze is restricted to the data
  engineering / compliance team only (enforced later via RBAC).
"""

import pandas as pd
from datetime import datetime

RAW_PATH = "raw/raw_retail_transactions.csv"
BRONZE_PATH = "bronze/retail_bronze.parquet"

def ingest_to_bronze(raw_path: str, bronze_path: str) -> pd.DataFrame:
    df = pd.read_csv(raw_path)

    # --- SECURITY CONTROL 1: HARD-DROP ---
    # CVV must NEVER be stored at rest, per PCI-DSS. It is dropped the moment
    # data lands in the lake, before anything is written to disk.
    if "cvv" in df.columns:
        df = df.drop(columns=["cvv"])
        print("[HARD-DROP] 'cvv' column permanently dropped at ingestion (PCI-DSS requirement).")

    # Add lineage / audit metadata
    df["_ingestion_timestamp"] = datetime.utcnow().isoformat()
    df["_source_system"] = "retail_pos_ecommerce"
    df["_layer"] = "bronze"

    df.to_parquet(bronze_path, index=False)
    print(f"[BRONZE] Ingested {df.shape[0]} rows, {df.shape[1]} columns -> {bronze_path}")
    print(f"[BRONZE] Columns retained: {list(df.columns)}")
    return df

if __name__ == "__main__":
    ingest_to_bronze(RAW_PATH, BRONZE_PATH)
