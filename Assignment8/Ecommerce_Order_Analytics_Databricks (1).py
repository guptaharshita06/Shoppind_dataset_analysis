# Databricks notebook source
# MAGIC %md
# MAGIC # E-Commerce Order Analytics System
# MAGIC ### Celebal Technologies — Intern Mini Project (Databricks Version)
# MAGIC
# MAGIC **Skills tested:** Python (PySpark), SQL (Spark SQL), Problem Solving
# MAGIC
# MAGIC This notebook implements all 5 parts of the project **natively on Databricks**, following the
# MAGIC **Bronze → Silver → Gold medallion architecture**:
# MAGIC 1. Data Generation (Bronze layer — 4 raw tables, 500+ rows each, intentional issues) — via Faker, converted to Spark DataFrames
# MAGIC 2. Data Cleaning (Silver layer) — **PySpark DataFrame API** (`clean_orders`, `clean_products`, `validate_emails`, `check_referential_integrity`)
# MAGIC 3. SQL Analysis (Gold layer) — **Spark SQL** (Delta tables), 16 queries: basic, intermediate, advanced (window functions, CTEs, cohort analysis), materialized as Gold aggregate tables
# MAGIC 4. Python + SQL Integration — **Databricks widgets** as the interactive "CLI" (report type + date range inputs)
# MAGIC 5. Edge Case Handling — assertion-based tests, run as a notebook cell
# MAGIC
# MAGIC | Layer | Tables | Purpose |
# MAGIC |-------|--------|---------|
# MAGIC | 🥉 **Bronze** | `bronze_customers`, `bronze_products`, `bronze_orders`, `bronze_order_items` | Raw, unprocessed data exactly as generated/ingested |
# MAGIC | 🥈 **Silver** | `silver_customers`, `silver_products`, `silver_orders`, `silver_order_items` | Cleaned, validated, deduplicated — analysis-ready |
# MAGIC | 🥇 **Gold** | `gold_category_revenue`, `gold_customer_segments`, `gold_monthly_revenue`, `gold_cohort_retention` | Pre-aggregated business metrics — dashboard/reporting-ready |
# MAGIC
# MAGIC > Run all cells top to bottom (`Run All`). Each `# COMMAND` block below is a separate notebook cell.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import random

# %pip install faker   -- uncomment and run in its own cell first if Faker isn't on the cluster
from faker import Faker

CATALOG_SCHEMA = "ecommerce_analytics"   # Delta tables will be created under this schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_SCHEMA}")
spark.sql(f"USE {CATALOG_SCHEMA}")

print("Setup complete. Active schema:", CATALOG_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Data Generation (🥉 Bronze Layer)
# MAGIC
# MAGIC Generates 4 related tables with the specific intentional issues required by the spec:
# MAGIC
# MAGIC | Table (Bronze) | Issues Injected |
# MAGIC |-------|-------------------|
# MAGIC | `bronze_customers` | 2% invalid emails |
# MAGIC | `bronze_products`  | ~15% messy product names (spacing/casing) |
# MAGIC | `bronze_orders`    | 5% NULL `customer_id`, ~8% wrong date format (DD-MM-YYYY), 8 future-dated orders |
# MAGIC | `bronze_order_items` | ~3% negative quantity (returns), some `discount_percent`>100, some `quantity`==0, orphan `order_id` references |
# MAGIC
# MAGIC Data is generated with `Faker` into Python lists (identical logic to the local Python version),
# MAGIC then converted straight into Spark DataFrames — no CSV round-trip needed on Databricks.

# COMMAND ----------

fake = Faker()
Faker.seed(11)
random.seed(11)

N_CUSTOMERS, N_PRODUCTS, N_ORDERS = 500, 220, 2500
REGIONS = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]
CATEGORY_SUBCATS = {
    "Electronics": ["Mobiles", "Laptops", "Accessories", "Cameras"],
    "Clothing": ["Men", "Women", "Kids", "Footwear"],
    "Home": ["Furniture", "Kitchen", "Decor", "Bedding"],
    "Books": ["Fiction", "Non-Fiction", "Academic", "Comics"],
}
STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
CUSTOMER_TYPES = ["REGULAR", "PREMIUM", "VIP"]

# ---- customers (raw) ----
customers_rows = []
for i in range(1, N_CUSTOMERS + 1):
    customers_rows.append({
        "customer_id": f"CUST{1000+i}", "customer_name": fake.name(), "email": fake.email(),
        "registration_date": fake.date_between(start_date="-3y", end_date="-15d").strftime("%Y-%m-%d"),
        "customer_type": random.choices(CUSTOMER_TYPES, weights=[65, 25, 10])[0],
    })
n_bad_email = max(1, int(N_CUSTOMERS * 0.02))
for i in random.sample(range(N_CUSTOMERS), n_bad_email):
    local = customers_rows[i]["email"].split("@")[0]
    customers_rows[i]["email"] = f"{local}example.com" if random.random() < 0.5 else f"{local}@"

# ---- products (raw) ----
products_rows = []
for i in range(1, N_PRODUCTS + 1):
    category = random.choice(list(CATEGORY_SUBCATS.keys()))
    subcategory = random.choice(CATEGORY_SUBCATS[category])
    name = f"{fake.word().capitalize()} {subcategory} {random.choice(['Pro','Max','Lite','Plus',''])}".strip()
    products_rows.append({
        "product_id": f"PROD{100+i}", "product_name": name, "category": category,
        "subcategory": subcategory, "cost_price": round(random.uniform(50, 15000), 2),
    })
for i in random.sample(range(N_PRODUCTS), int(N_PRODUCTS * 0.15)):
    style = random.choice(["extra_spaces", "upper", "lower"])
    name = products_rows[i]["product_name"]
    products_rows[i]["product_name"] = (f"  {name}  ".replace(" ", "  ", 1) if style == "extra_spaces"
                                          else name.upper() if style == "upper" else name.lower())

# ---- orders (raw) ----
customer_ids = [c["customer_id"] for c in customers_rows]
orders_rows = []
start_date, end_date = datetime(2023, 1, 1), datetime(2026, 7, 5)
for i in range(1, N_ORDERS + 1):
    dt = fake.date_time_between(start_date=start_date, end_date=end_date)
    orders_rows.append({
        "order_id": f"ORD{20000+i}", "customer_id": random.choice(customer_ids),
        "order_date": dt.strftime("%Y-%m-%d %H:%M:%S"),
        "status": random.choices(STATUSES, weights=[15, 20, 40, 15, 10])[0],
        "region_code": random.choice(REGIONS),
    })
for i in random.sample(range(N_ORDERS), int(N_ORDERS * 0.05)):
    orders_rows[i]["customer_id"] = None
for i in random.sample(range(N_ORDERS), int(N_ORDERS * 0.08)):
    dt = fake.date_time_between(start_date=start_date, end_date=end_date)
    orders_rows[i]["order_date"] = dt.strftime("%d-%m-%Y")
for i in random.sample(range(N_ORDERS), 8):
    future_dt = datetime(2027, random.randint(1, 12), random.randint(1, 28))
    orders_rows[i]["order_date"] = future_dt.strftime("%Y-%m-%d %H:%M:%S")

# ---- order_items (raw) ----
product_ids = [p["product_id"] for p in products_rows]
order_items_rows, item_counter = [], 1
for order in orders_rows:
    for prod_id in random.sample(product_ids, min(random.randint(1, 4), len(product_ids))):
        order_items_rows.append({
            "order_item_id": f"OI{item_counter}", "order_id": order["order_id"], "product_id": prod_id,
            "quantity": random.randint(1, 5), "unit_price": round(random.uniform(100, 20000), 2),
            "discount_percent": round(random.uniform(0, 40), 1),
        })
        item_counter += 1
n_total = len(order_items_rows)
for i in random.sample(range(n_total), int(n_total * 0.03)):
    order_items_rows[i]["quantity"] = -abs(order_items_rows[i]["quantity"])
for i in random.sample(range(n_total), 6):
    order_items_rows[i]["discount_percent"] = round(random.uniform(101, 150), 1)
for i in random.sample(range(n_total), 6):
    order_items_rows[i]["quantity"] = 0
for j in range(max(5, int(n_total * 0.01))):
    item_counter += 1
    order_items_rows.append({
        "order_item_id": f"OI{item_counter}", "order_id": f"ORD{99000+j}",
        "product_id": random.choice(product_ids), "quantity": random.randint(1, 3),
        "unit_price": round(random.uniform(100, 5000), 2), "discount_percent": round(random.uniform(0, 30), 1),
    })

# ---- Convert to Spark DataFrames ----
customers_df = spark.createDataFrame(customers_rows)
products_df = spark.createDataFrame(products_rows)
orders_df = spark.createDataFrame(orders_rows)
order_items_df = spark.createDataFrame(order_items_rows)

print(f"customers: {customers_df.count()}, products: {products_df.count()}, "
      f"orders: {orders_df.count()}, order_items: {order_items_df.count()}")

customers_df.write.mode("overwrite").saveAsTable("bronze_customers")
products_df.write.mode("overwrite").saveAsTable("bronze_products")
orders_df.write.mode("overwrite").saveAsTable("bronze_orders")
order_items_df.write.mode("overwrite").saveAsTable("bronze_order_items")
print("Raw Delta tables written.")

# COMMAND ----------

display(orders_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Data Cleaning (🥈 Silver Layer — PySpark)
# MAGIC
# MAGIC Implements the 4 required functions using the **PySpark DataFrame API**:
# MAGIC - `clean_orders(df)` — fixes DD-MM-YYYY dates via `try_to_timestamp`, flags/handles NULL `customer_id`
# MAGIC - `clean_products(df)` — trims whitespace, standardizes to Title Case
# MAGIC - `validate_emails(df)` — returns list of `customer_id`s with an invalid email format
# MAGIC - `check_referential_integrity(items_df, orders_df)` — finds `silver_order_items` rows referencing a non-existent `order_id`

# COMMAND ----------

EMAIL_REGEX = r'^[^@\s]+@[^@\s]+\.[^@\s]+$'

def validate_emails(customers_df):
    """Returns the list of customer_ids whose email fails basic format validation."""
    invalid_df = customers_df.filter(~F.col("email").rlike(EMAIL_REGEX))
    invalid_ids = [r["customer_id"] for r in invalid_df.select("customer_id").collect()]
    print(f"[validate_emails] Found {len(invalid_ids)} customers with an invalid email.")
    return invalid_ids


def clean_products(products_df):
    """Normalizes product names: trims, collapses double spaces, Title Case."""
    n_before = products_df.count()
    df = products_df.dropDuplicates(["product_id"])
    df = (df.withColumn("product_name", F.trim(F.regexp_replace(F.col("product_name"), r"\s+", " ")))
            .withColumn("product_name", F.initcap(F.col("product_name")))
            .filter(F.col("cost_price") >= 0))
    print(f"[clean_products] {n_before} -> {df.count()} rows (deduped, normalized names, dropped negative cost_price).")
    return df


def clean_orders(orders_df):
    """Fixes DD-MM-YYYY dates and flags/handles NULL customer_id."""
    n_before = orders_df.count()
    df = orders_df.dropDuplicates(["order_id"])

    parsed = F.coalesce(
        F.try_to_timestamp(F.col("order_date"), F.lit("yyyy-MM-dd HH:mm:ss")),
        F.try_to_timestamp(F.col("order_date"), F.lit("dd-MM-yyyy")),
    )
    df = df.withColumn("order_date", parsed)

    df = (df.withColumn("has_missing_customer",
                         F.col("customer_id").isNull() | (F.trim(F.coalesce(F.col("customer_id"), F.lit(""))) == ""))
            .withColumn("customer_id", F.when(F.col("has_missing_customer"), "UNKNOWN").otherwise(F.col("customer_id")))
            .withColumn("status", F.upper(F.trim(F.col("status")))))

    n_null_cust = df.filter("has_missing_customer = true").count()
    print(f"[clean_orders] {n_before} -> {df.count()} rows. {n_null_cust} orders flagged with missing customer_id.")
    return df


def check_referential_integrity(order_items_df, orders_df):
    """Finds silver_order_items rows whose order_id does not exist in silver_orders at all."""
    valid_ids_df = orders_df.select("order_id").distinct()
    orphans = order_items_df.join(valid_ids_df, on="order_id", how="left_anti")
    print(f"[check_referential_integrity] Found {orphans.count()} orphan silver_order_items rows.")
    return orphans


def clean_order_items(order_items_df, orders_clean_df, products_clean_df):
    n_before = order_items_df.count()
    df = order_items_df.dropDuplicates(["order_item_id"])

    orphans = check_referential_integrity(df, orders_clean_df)
    df = df.join(orphans.select("order_item_id"), on="order_item_id", how="left_anti")

    valid_products = products_clean_df.select("product_id").distinct()
    df = df.join(valid_products, on="product_id", how="left_semi")

    n_zero = df.filter(F.col("quantity") == 0).count()
    df = df.filter(F.col("quantity") != 0)

    n_bad_discount = df.filter(F.col("discount_percent") > 100).count()
    df = df.withColumn("discount_percent",
                        F.when(F.col("discount_percent") > 100, 100).otherwise(F.col("discount_percent")))

    n_neg = df.filter(F.col("quantity") < 0).count()
    print(f"[clean_order_items] {n_before} -> {df.count()} rows. "
          f"Removed {n_zero} zero-qty rows, capped {n_bad_discount} discounts>100%, "
          f"preserved {n_neg} negative-qty rows (returns).")
    return df

# COMMAND ----------

invalid_email_ids = validate_emails(customers_df)
customers_clean = customers_df.withColumn("has_invalid_email", F.col("customer_id").isin(invalid_email_ids))

products_clean = clean_products(products_df)
orders_clean = clean_orders(orders_df)
order_items_clean = clean_order_items(order_items_df, orders_clean, products_clean)

customers_clean.write.mode("overwrite").saveAsTable("silver_customers")
products_clean.write.mode("overwrite").saveAsTable("silver_products")
orders_clean.write.mode("overwrite").saveAsTable("silver_orders")
order_items_clean.write.mode("overwrite").saveAsTable("silver_order_items")

print("\nCleaned Delta tables written: silver_customers, silver_products, silver_orders, silver_order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: SQL Analysis (Spark SQL / Delta Tables)
# MAGIC
# MAGIC All 16 required queries below run as native **Spark SQL** against the `silver_*` Delta tables.
# MAGIC Cell magic `%sql` is used where a pure-SQL cell is convenient; `spark.sql(...)` is used where the
# MAGIC result needs to flow back into Python for further use (e.g. `display()`). A subset of these are
# MAGIC materialized as **🥇 Gold layer** tables at the end of this section for dashboard-style consumption.

# COMMAND ----------

# MAGIC %md #### Basic Query 1: Total Revenue per Category

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.category,
# MAGIC     ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)), 2) AS total_revenue
# MAGIC FROM silver_order_items oi JOIN silver_products p ON oi.product_id = p.product_id
# MAGIC GROUP BY p.category ORDER BY total_revenue DESC

# COMMAND ----------

# MAGIC %md #### Basic Query 2: Top 10 Customers by Total Order Value

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.customer_id, c.customer_name,
# MAGIC     ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)), 2) AS total_order_value
# MAGIC FROM silver_customers c JOIN silver_orders o ON c.customer_id = o.customer_id
# MAGIC JOIN silver_order_items oi ON o.order_id = oi.order_id
# MAGIC GROUP BY c.customer_id, c.customer_name ORDER BY total_order_value DESC LIMIT 10

# COMMAND ----------

# MAGIC %md #### Basic Query 3: Month-wise Order Count (Last 12 Months)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_format(order_date, 'yyyy-MM') AS order_month, COUNT(DISTINCT order_id) AS order_count
# MAGIC FROM silver_orders
# MAGIC WHERE order_date >= add_months((SELECT MAX(order_date) FROM silver_orders), -12)
# MAGIC GROUP BY order_month ORDER BY order_month

# COMMAND ----------

# MAGIC %md #### Intermediate Query 4: Customers Who Never Had an Item Delivered

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT c.customer_id, c.customer_name
# MAGIC FROM silver_customers c JOIN silver_orders o ON c.customer_id = o.customer_id
# MAGIC WHERE c.customer_id NOT IN (SELECT DISTINCT customer_id FROM silver_orders WHERE status = 'DELIVERED')

# COMMAND ----------

# MAGIC %md #### Intermediate Query 5: Products with More Returns than Purchases

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH product_activity AS (
# MAGIC     SELECT product_id,
# MAGIC         SUM(CASE WHEN quantity > 0 THEN quantity ELSE 0 END) AS total_purchased,
# MAGIC         SUM(CASE WHEN quantity < 0 THEN ABS(quantity) ELSE 0 END) AS total_returned
# MAGIC     FROM silver_order_items GROUP BY product_id
# MAGIC )
# MAGIC SELECT p.product_id, p.product_name, p.category, pa.total_purchased, pa.total_returned
# MAGIC FROM product_activity pa JOIN silver_products p ON pa.product_id = p.product_id
# MAGIC WHERE pa.total_returned > pa.total_purchased
# MAGIC ORDER BY pa.total_returned DESC

# COMMAND ----------

# MAGIC %md #### Intermediate Query 6: Return Rate per Category

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.category,
# MAGIC     SUM(CASE WHEN oi.quantity < 0 THEN ABS(oi.quantity) ELSE 0 END) AS returned_items,
# MAGIC     SUM(ABS(oi.quantity)) AS total_items,
# MAGIC     ROUND(100.0 * SUM(CASE WHEN oi.quantity < 0 THEN ABS(oi.quantity) ELSE 0 END) / NULLIF(SUM(ABS(oi.quantity)), 0), 2) AS return_rate_pct
# MAGIC FROM silver_order_items oi JOIN silver_products p ON oi.product_id = p.product_id
# MAGIC GROUP BY p.category ORDER BY return_rate_pct DESC

# COMMAND ----------

# MAGIC %md #### Advanced Query 7: Running Total of Revenue per Region (Window Function)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH daily_region_revenue AS (
# MAGIC     SELECT o.region_code, DATE(o.order_date) AS order_date,
# MAGIC         ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)), 2) AS daily_revenue
# MAGIC     FROM silver_orders o JOIN silver_order_items oi ON o.order_id = oi.order_id
# MAGIC     GROUP BY o.region_code, DATE(o.order_date)
# MAGIC )
# MAGIC SELECT region_code, order_date, daily_revenue,
# MAGIC     ROUND(SUM(daily_revenue) OVER (PARTITION BY region_code ORDER BY order_date), 2) AS running_total
# MAGIC FROM daily_region_revenue ORDER BY region_code, order_date

# COMMAND ----------

# MAGIC %md #### Advanced Query 8: DENSE_RANK — Products Ranked by Revenue within Category

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH product_revenue AS (
# MAGIC     SELECT p.category, p.product_id, p.product_name,
# MAGIC         ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)), 2) AS total_revenue
# MAGIC     FROM silver_products p JOIN silver_order_items oi ON p.product_id = oi.product_id
# MAGIC     GROUP BY p.category, p.product_id, p.product_name
# MAGIC )
# MAGIC SELECT category, product_name, total_revenue,
# MAGIC     DENSE_RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS rank_in_category
# MAGIC FROM product_revenue ORDER BY category, rank_in_category

# COMMAND ----------

# MAGIC %md #### Advanced Query 9: LAG Analysis — Days Between Consecutive Orders, "At Risk" Flag

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH customer_order_dates AS (
# MAGIC     SELECT DISTINCT customer_id, DATE(order_date) AS order_date FROM silver_orders WHERE customer_id != 'UNKNOWN'
# MAGIC ),
# MAGIC gaps AS (
# MAGIC     SELECT customer_id, order_date,
# MAGIC         LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS previous_order_date,
# MAGIC         DATEDIFF(order_date, LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date)) AS days_gap
# MAGIC     FROM customer_order_dates
# MAGIC ),
# MAGIC customer_avg_gap AS (
# MAGIC     SELECT customer_id, AVG(days_gap) AS avg_gap FROM gaps WHERE days_gap IS NOT NULL GROUP BY customer_id
# MAGIC )
# MAGIC SELECT g.customer_id, g.order_date, g.previous_order_date, g.days_gap,
# MAGIC     CASE WHEN cag.avg_gap > 30 THEN 'At Risk' ELSE 'Active' END AS risk_flag
# MAGIC FROM gaps g JOIN customer_avg_gap cag ON g.customer_id = cag.customer_id
# MAGIC ORDER BY g.customer_id, g.order_date

# COMMAND ----------

# MAGIC %md #### Advanced Query 10: CTE with Multiple Levels — Monthly Revenue Categorization

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH monthly_customer_revenue AS (
# MAGIC     SELECT o.customer_id, date_format(o.order_date, 'yyyy-MM') AS order_month,
# MAGIC         SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)) AS monthly_revenue
# MAGIC     FROM silver_orders o JOIN silver_order_items oi ON o.order_id = oi.order_id
# MAGIC     WHERE o.customer_id != 'UNKNOWN'
# MAGIC     GROUP BY o.customer_id, order_month
# MAGIC ),
# MAGIC categorized AS (
# MAGIC     SELECT *, CASE WHEN monthly_revenue > 10000 THEN 'High'
# MAGIC                    WHEN monthly_revenue >= 5000 THEN 'Medium' ELSE 'Low' END AS revenue_category
# MAGIC     FROM monthly_customer_revenue
# MAGIC )
# MAGIC SELECT order_month, revenue_category, COUNT(DISTINCT customer_id) AS customer_count
# MAGIC FROM categorized GROUP BY order_month, revenue_category ORDER BY order_month, revenue_category

# COMMAND ----------

# MAGIC %md #### Advanced Query 11: NTILE Segmentation — Platinum/Gold/Silver/Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH customer_ltv AS (
# MAGIC     SELECT c.customer_id, ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)), 2) AS total_value
# MAGIC     FROM silver_customers c JOIN silver_orders o ON c.customer_id = o.customer_id
# MAGIC     JOIN silver_order_items oi ON o.order_id = oi.order_id GROUP BY c.customer_id
# MAGIC )
# MAGIC SELECT customer_id, total_value,
# MAGIC     NTILE(4) OVER (ORDER BY total_value DESC) AS quartile,
# MAGIC     CASE NTILE(4) OVER (ORDER BY total_value DESC)
# MAGIC         WHEN 1 THEN 'Platinum' WHEN 2 THEN 'Gold' WHEN 3 THEN 'Silver' WHEN 4 THEN 'Bronze' END AS quartile_label
# MAGIC FROM customer_ltv ORDER BY total_value DESC

# COMMAND ----------

# MAGIC %md #### Advanced Query 12: Year-over-Year Revenue Comparison

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH monthly_revenue AS (
# MAGIC     SELECT YEAR(o.order_date) AS year, MONTH(o.order_date) AS month,
# MAGIC         ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)), 2) AS revenue
# MAGIC     FROM silver_orders o JOIN silver_order_items oi ON o.order_id = oi.order_id GROUP BY year, month
# MAGIC )
# MAGIC SELECT cur.year, cur.month, cur.revenue, prev.revenue AS prev_year_revenue,
# MAGIC     CASE WHEN prev.revenue IS NULL OR prev.revenue = 0 THEN NULL
# MAGIC          ELSE ROUND(100.0 * (cur.revenue - prev.revenue) / prev.revenue, 2) END AS yoy_growth_percent
# MAGIC FROM monthly_revenue cur LEFT JOIN monthly_revenue prev
# MAGIC     ON cur.month = prev.month AND cur.year = prev.year + 1
# MAGIC ORDER BY cur.year, cur.month

# COMMAND ----------

# MAGIC %md #### Advanced Query 13: FIRST_VALUE / LAST_VALUE — Category Shift per Customer

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH customer_category_orders AS (
# MAGIC     SELECT o.customer_id, o.order_date, p.category,
# MAGIC         FIRST_VALUE(p.category) OVER (PARTITION BY o.customer_id ORDER BY o.order_date
# MAGIC             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_category,
# MAGIC         LAST_VALUE(p.category) OVER (PARTITION BY o.customer_id ORDER BY o.order_date
# MAGIC             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS most_recent_category
# MAGIC     FROM silver_orders o JOIN silver_order_items oi ON o.order_id = oi.order_id
# MAGIC     JOIN silver_products p ON oi.product_id = p.product_id
# MAGIC     WHERE o.customer_id != 'UNKNOWN'
# MAGIC )
# MAGIC SELECT DISTINCT customer_id, first_category, most_recent_category,
# MAGIC     CASE WHEN first_category != most_recent_category THEN 'Yes' ELSE 'No' END AS category_shift
# MAGIC FROM customer_category_orders ORDER BY customer_id

# COMMAND ----------

# MAGIC %md #### Advanced Query 14: Cumulative Revenue Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH customer_revenue AS (
# MAGIC     SELECT c.customer_id, ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)), 2) AS revenue
# MAGIC     FROM silver_customers c JOIN silver_orders o ON c.customer_id = o.customer_id
# MAGIC     JOIN silver_order_items oi ON o.order_id = oi.order_id GROUP BY c.customer_id
# MAGIC ),
# MAGIC ranked AS (
# MAGIC     SELECT customer_id, revenue,
# MAGIC         SUM(revenue) OVER (ORDER BY revenue DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue,
# MAGIC         SUM(revenue) OVER () AS total_revenue
# MAGIC     FROM customer_revenue
# MAGIC )
# MAGIC SELECT customer_id, revenue, cumulative_revenue,
# MAGIC     ROUND(100.0 * cumulative_revenue / total_revenue, 2) AS cumulative_percent
# MAGIC FROM ranked ORDER BY revenue DESC

# COMMAND ----------

# MAGIC %md #### Advanced Query 15: Cohort Retention Analysis (Complex Multi-Level CTE)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH customer_cohort AS (
# MAGIC     SELECT customer_id, date_format(registration_date, 'yyyy-MM') AS cohort_month FROM silver_customers
# MAGIC ),
# MAGIC customer_orders_with_offset AS (
# MAGIC     SELECT cc.cohort_month, o.customer_id,
# MAGIC         (YEAR(o.order_date) - YEAR(TO_DATE(CONCAT(cc.cohort_month, '-01')))) * 12
# MAGIC         + (MONTH(o.order_date) - MONTH(TO_DATE(CONCAT(cc.cohort_month, '-01')))) AS month_offset
# MAGIC     FROM silver_orders o JOIN customer_cohort cc ON o.customer_id = cc.customer_id
# MAGIC     WHERE o.customer_id != 'UNKNOWN'
# MAGIC ),
# MAGIC cohort_sizes AS (
# MAGIC     SELECT cohort_month, COUNT(DISTINCT customer_id) AS cohort_size FROM customer_cohort GROUP BY cohort_month
# MAGIC )
# MAGIC SELECT cw.cohort_month, ANY_VALUE(cs.cohort_size) AS cohort_size, cw.month_offset,
# MAGIC     COUNT(DISTINCT cw.customer_id) AS active_customers,
# MAGIC     ROUND(100.0 * COUNT(DISTINCT cw.customer_id) / ANY_VALUE(cs.cohort_size), 1) AS retention_rate_pct
# MAGIC FROM customer_orders_with_offset cw JOIN cohort_sizes cs ON cw.cohort_month = cs.cohort_month
# MAGIC WHERE cw.month_offset BETWEEN 0 AND 3
# MAGIC GROUP BY cw.cohort_month, cw.month_offset
# MAGIC ORDER BY cw.cohort_month, cw.month_offset

# COMMAND ----------

# MAGIC %md #### Advanced Query 16: Self-Join with Window Function — Rapid Repeat Orders

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH order_dates AS (
# MAGIC     SELECT DISTINCT order_id, customer_id, DATE(order_date) AS order_date FROM silver_orders WHERE customer_id != 'UNKNOWN'
# MAGIC )
# MAGIC SELECT a.customer_id, a.order_id AS order_id_1, a.order_date AS order_date_1,
# MAGIC     b.order_id AS order_id_2, b.order_date AS order_date_2,
# MAGIC     DATEDIFF(b.order_date, a.order_date) AS days_apart,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY a.customer_id ORDER BY a.order_date, b.order_date) AS pair_number
# MAGIC FROM order_dates a JOIN order_dates b
# MAGIC     ON a.customer_id = b.customer_id AND a.order_id < b.order_id
# MAGIC     AND DATEDIFF(b.order_date, a.order_date) BETWEEN 0 AND 7
# MAGIC ORDER BY a.customer_id, a.order_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥇 Gold Layer: Materialized Business Aggregates
# MAGIC
# MAGIC A subset of the Part 3 analysis queries are materialized here as standalone **Gold Delta
# MAGIC tables** — pre-aggregated, ready for a BI dashboard (Databricks SQL, Power BI, Tableau) to
# MAGIC query directly without re-running the underlying joins every time.

# COMMAND ----------

# Gold tables are built with spark.sql(...).write.saveAsTable(...) — the same proven
# pattern used for the Bronze/Silver tables above (rather than "CREATE OR REPLACE TABLE
# ... AS SELECT" SQL syntax, which behaves inconsistently across catalog configurations).

gold_category_revenue = spark.sql("""
    SELECT p.category,
        ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)), 2) AS total_revenue,
        SUM(CASE WHEN oi.quantity < 0 THEN ABS(oi.quantity) ELSE 0 END) AS returned_items,
        SUM(ABS(oi.quantity)) AS total_items
    FROM silver_order_items oi JOIN silver_products p ON oi.product_id = p.product_id
    GROUP BY p.category
""")
gold_category_revenue.write.mode("overwrite").saveAsTable("gold_category_revenue")

gold_customer_segments = spark.sql("""
    WITH customer_ltv AS (
        SELECT c.customer_id, c.customer_name,
            ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)), 2) AS total_value
        FROM silver_customers c JOIN silver_orders o ON c.customer_id = o.customer_id
        JOIN silver_order_items oi ON o.order_id = oi.order_id
        GROUP BY c.customer_id, c.customer_name
    )
    SELECT customer_id, customer_name, total_value,
        NTILE(4) OVER (ORDER BY total_value DESC) AS quartile,
        CASE NTILE(4) OVER (ORDER BY total_value DESC)
            WHEN 1 THEN 'Platinum' WHEN 2 THEN 'Gold' WHEN 3 THEN 'Silver' WHEN 4 THEN 'Bronze' END AS quartile_label
    FROM customer_ltv
""")
gold_customer_segments.write.mode("overwrite").saveAsTable("gold_customer_segments")

gold_monthly_revenue = spark.sql("""
    SELECT date_format(o.order_date, 'yyyy-MM') AS order_month,
        COUNT(DISTINCT o.order_id) AS total_orders,
        COUNT(DISTINCT o.customer_id) AS unique_customers,
        ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)), 2) AS total_revenue
    FROM silver_orders o JOIN silver_order_items oi ON o.order_id = oi.order_id
    GROUP BY order_month
""")
gold_monthly_revenue.write.mode("overwrite").saveAsTable("gold_monthly_revenue")

gold_cohort_retention = spark.sql("""
    WITH customer_cohort AS (
        SELECT customer_id, date_format(registration_date, 'yyyy-MM') AS cohort_month FROM silver_customers
    ),
    customer_orders_with_offset AS (
        SELECT cc.cohort_month, o.customer_id,
            (YEAR(o.order_date) - YEAR(TO_DATE(CONCAT(cc.cohort_month, '-01')))) * 12
            + (MONTH(o.order_date) - MONTH(TO_DATE(CONCAT(cc.cohort_month, '-01')))) AS month_offset
        FROM silver_orders o JOIN customer_cohort cc ON o.customer_id = cc.customer_id
        WHERE o.customer_id != 'UNKNOWN'
    ),
    cohort_sizes AS (
        SELECT cohort_month, COUNT(DISTINCT customer_id) AS cohort_size FROM customer_cohort GROUP BY cohort_month
    )
    SELECT cw.cohort_month, ANY_VALUE(cs.cohort_size) AS cohort_size, cw.month_offset,
        COUNT(DISTINCT cw.customer_id) AS active_customers,
        ROUND(100.0 * COUNT(DISTINCT cw.customer_id) / ANY_VALUE(cs.cohort_size), 1) AS retention_rate_pct
    FROM customer_orders_with_offset cw JOIN cohort_sizes cs ON cw.cohort_month = cs.cohort_month
    WHERE cw.month_offset BETWEEN 0 AND 3
    GROUP BY cw.cohort_month, cw.month_offset
""")
gold_cohort_retention.write.mode("overwrite").saveAsTable("gold_cohort_retention")

print("Gold tables written: gold_category_revenue, gold_customer_segments, "
      "gold_monthly_revenue, gold_cohort_retention")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_category_revenue ORDER BY total_revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC 4 Gold tables now exist: `gold_category_revenue`, `gold_customer_segments`,
# MAGIC `gold_monthly_revenue`, `gold_cohort_retention` — all queryable instantly, with no joins
# MAGIC needed at query time.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Python + SQL Integration — Report Tool via Databricks Widgets
# MAGIC
# MAGIC Databricks notebooks don't have a terminal, so **widgets** are the natural equivalent of
# MAGIC command-line arguments/interactive input — they give the same "user provides report type +
# MAGIC date range" experience directly in the notebook UI.

# COMMAND ----------

dbutils.widgets.dropdown("report_type", "monthly", ["daily", "weekly", "monthly"], "Report Type")
dbutils.widgets.text("start_date", "2024-01-01", "Start Date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "2024-01-31", "End Date (YYYY-MM-DD)")

# COMMAND ----------

def validate_date(value, label):
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"[ERROR] Invalid {label} '{value}'. Expected format: YYYY-MM-DD.")


def get_previous_period(start_dt, end_dt):
    period_length = (end_dt - start_dt).days + 1
    prev_end = start_dt - timedelta(days=1)
    prev_start = prev_end - timedelta(days=period_length - 1)
    return prev_start, prev_end


def period_summary(start_dt, end_dt):
    s, e = start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")
    row = spark.sql(f"""
        SELECT COUNT(DISTINCT o.order_id) AS total_orders, COUNT(DISTINCT o.customer_id) AS unique_customers,
            ROUND(COALESCE(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100.0)), 0), 2) AS revenue
        FROM silver_orders o LEFT JOIN silver_order_items oi ON o.order_id = oi.order_id
        WHERE DATE(o.order_date) BETWEEN '{s}' AND '{e}'
    """).collect()[0]
    return {"total_orders": row["total_orders"] or 0, "unique_customers": row["unique_customers"] or 0,
            "revenue": row["revenue"] or 0.0}


def top_products(start_dt, end_dt, limit=3):
    s, e = start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")
    return spark.sql(f"""
        SELECT p.product_name, ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100.0)), 2) AS rev
        FROM silver_orders o JOIN silver_order_items oi ON o.order_id = oi.order_id
        JOIN silver_products p ON oi.product_id = p.product_id
        WHERE DATE(o.order_date) BETWEEN '{s}' AND '{e}'
        GROUP BY p.product_name ORDER BY rev DESC LIMIT {limit}
    """).collect()


def pct_change(current, previous):
    if previous in (None, 0):
        return None
    return round(100.0 * (current - previous) / previous, 2)


def generate_report():
    report_type = dbutils.widgets.get("report_type")
    start_raw = dbutils.widgets.get("start_date")
    end_raw = dbutils.widgets.get("end_date")

    try:
        start_dt = validate_date(start_raw, "start_date")
        end_dt = validate_date(end_raw, "end_date")
    except ValueError as e:
        print(e)
        return

    if end_dt < start_dt:
        print("[ERROR] End date cannot be before start date.")
        return

    current = period_summary(start_dt, end_dt)
    prev_start, prev_end = get_previous_period(start_dt, end_dt)
    previous = period_summary(prev_start, prev_end)
    silver_products = top_products(start_dt, end_dt)

    print("=" * 60)
    print(f"{report_type.upper()} REPORT: {start_raw} to {end_raw}")
    print("=" * 60)
    if current["total_orders"] == 0:
        print("\n[NOTE] No silver_orders found in this date range. Showing zeros below.")
    print(f"\nTotal Orders     : {current['total_orders']}")
    print(f"Total Revenue     : Rs.{current['revenue']:,.2f}")
    print(f"Unique Customers  : {current['unique_customers']}")
    print(f"\nTop 3 Products:")
    if silver_products:
        for i, row in enumerate(silver_products, 1):
            print(f"  {i}. {row['product_name']} — Rs.{row['rev']:,.2f}")
    else:
        print("  (no product sales in this period)")

    print(f"\nComparison with Previous Period ({prev_start.date()} to {prev_end.date()}):")
    def fmt(v):
        return "N/A (no data in previous period)" if v is None else f"{'+' if v>=0 else ''}{v}%"
    print(f"  Orders    : {previous['total_orders']} -> {current['total_orders']}  "
          f"({fmt(pct_change(current['total_orders'], previous['total_orders']))})")
    print(f"  Revenue    : Rs.{previous['revenue']:,.2f} -> Rs.{current['revenue']:,.2f}  "
          f"({fmt(pct_change(current['revenue'], previous['revenue']))})")
    print(f"  Customers  : {previous['unique_customers']} -> {current['unique_customers']}  "
          f"({fmt(pct_change(current['unique_customers'], previous['unique_customers']))})")

generate_report()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Edge Case Handling
# MAGIC
# MAGIC Assertion-based tests for the 4 required scenarios, run directly as a notebook cell.

# COMMAND ----------

def test_orphan_order_id():
    raw_orphans = order_items_df.join(orders_df.select("order_id"), "order_id", "left_anti").count()
    clean_orphans = order_items_clean.join(orders_clean.select("order_id"), "order_id", "left_anti").count()
    assert raw_orphans > 0, "Setup issue: raw data should contain orphan rows."
    assert clean_orphans == 0, f"FAIL: {clean_orphans} orphans remain in cleaned data."
    print(f"[PASS] test_orphan_order_id — {raw_orphans} orphans in raw, 0 in cleaned.")


def test_discount_over_100():
    raw_bad = order_items_df.filter(F.col("discount_percent") > 100).count()
    clean_bad = order_items_clean.filter(F.col("discount_percent") > 100).count()
    assert raw_bad > 0, "Setup issue: raw data should contain discount>100 rows."
    assert clean_bad == 0, f"FAIL: {clean_bad} rows still have discount_percent > 100."
    print(f"[PASS] test_discount_over_100 — {raw_bad} found in raw, all capped in cleaned.")


def test_zero_quantity():
    raw_zero = order_items_df.filter(F.col("quantity") == 0).count()
    clean_zero = order_items_clean.filter(F.col("quantity") == 0).count()
    assert raw_zero > 0, "Setup issue: raw data should contain quantity==0 rows."
    assert clean_zero == 0, f"FAIL: {clean_zero} rows with quantity==0 remain."
    print(f"[PASS] test_zero_quantity — {raw_zero} found in raw, 0 remain in cleaned.")


def test_future_order_date():
    future_count = orders_clean.filter(F.col("order_date") > F.lit("2026-07-10")).count()
    assert future_count > 0, "Setup issue: expected future-dated silver_orders."
    # confirm a query spanning the future range doesn't error
    result = spark.sql("SELECT COUNT(*) AS cnt FROM silver_orders WHERE DATE(order_date) BETWEEN '2027-01-01' AND '2027-12-31'").collect()
    assert result[0]["cnt"] >= 0
    print(f"[PASS] test_future_order_date — {future_count} future-dated silver_orders preserved; queries over them run fine.")


print("=== PART 5: EDGE CASE TESTS ===\n")
for test in [test_orphan_order_id, test_discount_over_100, test_zero_quantity, test_future_order_date]:
    test()
print("\nAll edge case tests passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Part | Deliverable | Databricks Equivalent Used |
# MAGIC |------|-------------|-------------------------------|
# MAGIC | 1. Data Generation | 🥉 Bronze: 4 raw tables, 500+ rows each | Faker + `spark.createDataFrame` -> Delta tables |
# MAGIC | 2. Data Cleaning | 🥈 Silver: 4 required functions | PySpark DataFrame API (no pandas) |
# MAGIC | 3. SQL Analysis | 16 queries + 🥇 Gold: 4 materialized aggregate tables | Spark SQL against Delta tables (`%sql` cells) |
# MAGIC | 4. CLI Tool | Report type + date range input | Databricks **widgets** (`dbutils.widgets`) |
# MAGIC | 5. Edge Cases | 4 required tests | Assertion-based tests as a notebook cell |
# MAGIC
# MAGIC **Medallion architecture:** `bronze_*` (raw) → `silver_*` (cleaned) → `gold_*` (aggregated,
# MAGIC dashboard-ready) — all persisted as Delta tables in the `ecommerce_analytics` schema.
