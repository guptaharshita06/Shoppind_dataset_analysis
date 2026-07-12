# E-Commerce Order Analytics System — Databricks Version
### Celebal Technologies — Intern Mini Project (Assignment 8)

## 📌 Why this version exists
The original project spec says "local environment," but the mentor asked for this to be built
on **Databricks** instead. This version implements all 5 parts natively using **PySpark +
Spark SQL + Delta tables + Databricks widgets**, following a **Bronze → Silver → Gold
medallion architecture** — no pandas, no SQLite, no external CSV files.

## 📂 Dataset — There Is No File to Upload
This project does **not** use an external dataset (no CSV/Excel to upload anywhere). The
notebook **generates its own realistic e-commerce data** at runtime using the `Faker` library
— 4 related tables, 500+ rows each, with specific intentional data-quality issues built in
on purpose (so the cleaning logic has real problems to solve):

| Table | Rows | Intentional Issues |
|-------|------|----------------------|
| `customers` | 500 | 2% invalid emails |
| `products` | 220 | ~15% messy product names (extra spaces / inconsistent casing) |
| `orders` | 2,500 | 5% NULL `customer_id`, ~8% wrong date format (DD-MM-YYYY), 8 future-dated orders |
| `order_items` | ~6,300 | ~3% negative quantity (returns), a few `discount_percent`>100, a few `quantity`==0, orphan `order_id` references |

Everything — data generation, cleaning, the database, the queries, the report tool — runs
**inside the one notebook file**. You don't need to find or upload a dataset anywhere.

## 📥 What You Need to Upload / Submit
Just **one file**: `Ecommerce_Order_Analytics_Databricks.py`

- **To Databricks:** Import it as a notebook (Workspace → Import → File)
- **To GitHub:** Push it into the `Assignment8` folder (steps below)
- **To Celebal (if they want proof of execution):** Take screenshots of the notebook after
  `Run All`, or export it from Databricks as `.dbc`/`.html` (**File → Export** in the Databricks
  UI) and submit that alongside the `.py` source

## 🏗️ Architecture (Bronze → Silver → Gold)
| Layer | Tables | Purpose |
|-------|--------|---------|
| 🥉 **Bronze** | `bronze_customers`, `bronze_products`, `bronze_orders`, `bronze_order_items` | Raw, unprocessed data exactly as generated |
| 🥈 **Silver** | `silver_customers`, `silver_products`, `silver_orders`, `silver_order_items` | Cleaned, validated, deduplicated — analysis-ready |
| 🥇 **Gold** | `gold_category_revenue`, `gold_customer_segments`, `gold_monthly_revenue`, `gold_cohort_retention` | Pre-aggregated business metrics — dashboard-ready |

All 12 tables are Delta tables under the `ecommerce_analytics` schema.

## 🏗️ What Each Part Maps To
| Part | Local Python Version | Databricks Version |
|------|------------------------|----------------------|
| 1. Data Generation (Bronze) | Faker → CSV files | Faker → Python dicts → `spark.createDataFrame()` → Delta tables |
| 2. Data Cleaning (Silver) | Pandas (`clean_orders`, etc.) | **PySpark DataFrame API** — same function names/logic, no pandas |
| 3. SQL Analysis + Gold | SQLite (`sqlite3` + `queries.sql`) | **Spark SQL** `%sql` cells + materialized Gold Delta tables |
| 4. CLI Tool | `argparse` + `input()` | **Databricks widgets** (`dbutils.widgets`) |
| 5. Edge Cases | `assert`-based Python functions | Same, using PySpark DataFrame filters/counts |

## ⬆️ How to Run in Databricks
1. Log into your Databricks workspace (Community Edition is free: https://community.cloud.databricks.com)
2. **Workspace → your folder → Import → File** → upload `Ecommerce_Order_Analytics_Databricks.py`
3. Attach the notebook to a cluster
4. Add a cell at the very top with `%pip install faker` and run it (cluster restarts automatically)
5. **Run All**
6. Check **Data/Catalog** in the sidebar → `ecommerce_analytics` schema should show all 12 tables

## ✅ Validated Before Delivery
Every part of this notebook (data generation, PySpark cleaning functions, all 16 SQL query
patterns, the Gold layer tables, the widget-based report function, and all 4 edge case tests)
was run end-to-end against a local Spark session before delivery — confirmed working. Only
`dbutils.widgets` and `display()` require actual Databricks (they're stubbed identically
there, so no changes needed).

## 🔎 The 16 SQL Queries (Part 3)
Same business logic as the original local/SQLite version, translated to Spark SQL syntax:
`DATEDIFF()` instead of `julianday()` subtraction, `date_format()`/`YEAR()`/`MONTH()` instead
of `strftime()`, `ANY_VALUE()` for Spark's stricter `GROUP BY` rules, `try_to_timestamp()` for
safe date parsing. Basic (revenue per category, top customers, monthly order count),
Intermediate (never-delivered customers, high-return products, return rate), and Advanced
(running totals, DENSE_RANK, LAG/LEAD, multi-level CTEs, NTILE quartiles, YoY comparison,
FIRST_VALUE/LAST_VALUE, cumulative distribution, cohort retention, self-join + window function).

## 🧰 Tech Stack
PySpark (DataFrame API + Spark SQL), Delta Lake (managed tables), Databricks widgets, Faker

## ✍️ Author
Harshita — Data Engineering Intern, Celebal Technologies
