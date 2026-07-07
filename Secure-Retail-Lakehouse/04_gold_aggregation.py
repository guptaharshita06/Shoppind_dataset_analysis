"""
04_gold_aggregation.py
GOLD LAYER — Fully Anonymized, Analytics-Ready Aggregates

Purpose:
  Business analysts / data scientists get access ONLY to Gold. It contains
  zero direct or indirect identifiers — no names, no card data, no exact
  addresses, no row-level customer records at all. Only aggregated business
  metrics grouped by safe dimensions (region, category, age bucket, amount
  bucket, date). This satisfies the "principle of least privilege": analysts
  get the trends they need, nothing that could re-identify a person.
"""

import pandas as pd

SILVER_PATH = "silver/retail_silver.parquet"
GOLD_PATH = "gold/retail_gold_aggregated.csv"

def build_gold(silver_path: str, gold_path: str) -> pd.DataFrame:
    df = pd.read_parquet(silver_path)
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["transaction_month"] = df["order_date"].dt.to_period("M").astype(str)

    gold = (
        df.groupby(["transaction_month", "state", "category", "age_bucket", "amount_bucket"])
        .agg(
            total_transactions=("order_id", "count"),
            total_revenue=("sales", "sum"),
            total_profit=("profit", "sum"),
            avg_transaction_value=("sales", "mean"),
            unique_customers=("customer_id", "nunique"),
        )
        .reset_index()
    )
    gold["total_revenue"] = gold["total_revenue"].round(2)
    gold["total_profit"] = gold["total_profit"].round(2)
    gold["avg_transaction_value"] = gold["avg_transaction_value"].round(2)

    gold.to_csv(gold_path, index=False)
    print(f"[GOLD] Aggregated to {gold.shape[0]} rows, {gold.shape[1]} columns -> {gold_path}")
    print(f"[GOLD] Columns: {list(gold.columns)}")
    print("[GOLD] Zero direct/indirect identifiers present — safe for open analyst access.")
    return gold

if __name__ == "__main__":
    build_gold(SILVER_PATH, GOLD_PATH)
