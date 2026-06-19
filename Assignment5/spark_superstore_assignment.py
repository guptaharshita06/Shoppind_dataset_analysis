# =============================================================================
# CELEBAL TECHNOLOGIES – APACHE SPARK ASSIGNMENT
# Dataset : Sample - Superstore.csv  (Kaggle: vivek468/superstore-dataset-final)
# Objective: Data Cleaning, Transformation & Aggregation using PySpark
# Author  : Harshita | Celebal Technologies Internship
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType
)
import warnings
warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1 : SPARK SESSION
# ─────────────────────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("CelebalTech_Superstore_Spark_Harshita")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("=" * 70)
print("  CELEBAL TECHNOLOGIES – SPARK ASSIGNMENT | SUPERSTORE DATASET")
print("=" * 70)
print(f"  Spark Version : {spark.version}")
print(f"  App Name      : {spark.sparkContext.appName}")
print("=" * 70)

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2 : LOAD DATASET
# ─────────────────────────────────────────────────────────────────────────────
print("\n[SECTION 1] LOADING DATASET")
print("-" * 70)

CSV_PATH = "D:\Admin\Downloads\Superstoredataset3\Sample - Superstore.csv"

# Define exact schema matching Kaggle Superstore CSV
schema = StructType([
    StructField("Row_ID",        IntegerType(), True),
    StructField("Order_ID",      StringType(),  True),
    StructField("Order_Date",    StringType(),  True),   # will parse to date
    StructField("Ship_Date",     StringType(),  True),
    StructField("Ship_Mode",     StringType(),  True),
    StructField("Customer_ID",   StringType(),  True),
    StructField("Customer_Name", StringType(),  True),
    StructField("Segment",       StringType(),  True),
    StructField("Country",       StringType(),  True),
    StructField("City",          StringType(),  True),
    StructField("State",         StringType(),  True),
    StructField("Postal_Code",   StringType(),  True),
    StructField("Region",        StringType(),  True),
    StructField("Product_ID",    StringType(),  True),
    StructField("Category",      StringType(),  True),
    StructField("Sub_Category",  StringType(),  True),
    StructField("Product_Name",  StringType(),  True),
    StructField("Sales",         DoubleType(),  True),
    StructField("Quantity",      IntegerType(), True),
    StructField("Discount",      DoubleType(),  True),
    StructField("Profit",        DoubleType(),  True),
])

raw_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .schema(schema)
    .csv(CSV_PATH)
)

# Rename columns: remove spaces (already handled in schema above)
print(f"\n  Rows loaded  : {raw_df.count():,}")
print(f"  Columns      : {len(raw_df.columns)}")
print("\n  Schema:")
raw_df.printSchema()
print("  Sample rows (first 5):")
raw_df.show(5, truncate=True)

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3 : DATA CLEANING
# ─────────────────────────────────────────────────────────────────────────────
print("\n[SECTION 2] DATA CLEANING")
print("-" * 70)

# --- 2a. Null audit before cleaning ---
print("\n  [2a] NULL counts per column (raw):")
raw_df.select(
    [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in raw_df.columns]
).show()

# --- 2b. Remove duplicate rows ---
before_dedup = raw_df.count()
df_dedup = raw_df.dropDuplicates()
after_dedup = df_dedup.count()
print(f"  [2b] Duplicate rows removed : {before_dedup - after_dedup}  "
      f"({before_dedup:,} → {after_dedup:,})")

# --- 2c. Remove duplicate Order_ID + Product_ID combinations ---
df_dedup2 = df_dedup.dropDuplicates(["Order_ID", "Product_ID"])
print(f"  [2c] After Order+Product dedup: {df_dedup2.count():,} rows")

# --- 2d. Drop rows with NULL in critical columns ---
critical = ["Order_ID", "Customer_Name", "Sales", "Category", "Region"]
df_no_nulls = df_dedup2.dropna(subset=critical)
print(f"  [2d] After dropping critical NULLs: {df_no_nulls.count():,} rows")

# --- 2e. Handle inconsistent / empty string values ---
df_clean = (
    df_no_nulls
    .withColumn("Ship_Mode",
        F.when(F.trim(F.col("Ship_Mode")) == "", "Unknown")
         .otherwise(F.trim(F.col("Ship_Mode"))))
    .withColumn("Segment",
        F.when(F.trim(F.col("Segment")) == "", "Unknown")
         .otherwise(F.trim(F.col("Segment"))))
    .withColumn("Region",
        F.when(F.trim(F.col("Region")) == "", "Unknown")
         .otherwise(F.trim(F.col("Region"))))
    # Fill numeric NULLs with defaults
    .fillna({"Discount": 0.0, "Profit": 0.0, "Quantity": 1})
    # Remove negative Sales (data error)
    .filter(F.col("Sales") > 0)
    # Remove rows where Quantity is 0 or negative
    .filter(F.col("Quantity") > 0)
)

print(f"  [2e] After cleaning strings & invalid numerics: {df_clean.count():,} rows")

# --- 2f. Null audit after cleaning ---
print("\n  [2f] NULL counts after cleaning (should be 0 for critical cols):")
df_clean.select(
    [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in critical]
).show()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 4 : SCHEMA TRANSFORMATION – Casting & Renaming
# ─────────────────────────────────────────────────────────────────────────────
print("\n[SECTION 3] SCHEMA TRANSFORMATION")
print("-" * 70)

df_transformed = (
    df_clean
    # Cast date strings (DD/MM/YYYY) → DateType
    .withColumn("Order_Date", F.to_date(F.col("Order_Date"), "M/d/yyyy"))
    .withColumn("Ship_Date",  F.to_date(F.col("Ship_Date"),  "M/d/yyyy"))
    # Derive time features
    .withColumn("Order_Year",    F.year("Order_Date"))
    .withColumn("Order_Month",   F.month("Order_Date"))
    .withColumn("Order_Quarter", F.quarter("Order_Date"))
    # Days to ship
    .withColumn("Days_to_Ship",
        F.datediff(F.col("Ship_Date"), F.col("Order_Date")))
    # Discount as percentage integer label
    .withColumn("Discount_Pct",
        (F.col("Discount") * 100).cast(IntegerType()))
    # Revenue per unit
    .withColumn("Revenue_Per_Unit",
        F.round(F.col("Sales") / F.col("Quantity"), 2))
    # Profit margin %
    .withColumn("Profit_Margin_Pct",
        F.when(F.col("Sales") > 0,
               F.round((F.col("Profit") / F.col("Sales")) * 100, 2)
        ).otherwise(0.0))
    # Cast Postal_Code stays as string (leading zeros possible)
    # Round monetary cols to 2 dp
    .withColumn("Sales",  F.round("Sales", 2))
    .withColumn("Profit", F.round("Profit", 2))
    .drop("Discount")   # replaced by Discount_Pct
)

print("\n  Transformed Schema:")
df_transformed.printSchema()
print("  Sample (5 rows):")
df_transformed.select(
    "Order_ID","Order_Date","Order_Year","Order_Quarter",
    "Days_to_Ship","Category","Sub_Category","Region",
    "Sales","Quantity","Discount_Pct","Profit","Profit_Margin_Pct"
).show(5, truncate=True)

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 5 : FILTERING
# ─────────────────────────────────────────────────────────────────────────────
print("\n[SECTION 4] FILTERING")
print("-" * 70)

# 5a. Technology orders with Sales > 1000
tech_high = df_transformed.filter(
    (F.col("Category") == "Technology") & (F.col("Sales") > 1000)
)
print(f"\n  [4a] Technology + Sales > 1000 : {tech_high.count():,} rows")
tech_high.select("Order_ID","Customer_Name","Sub_Category","State","Sales","Profit").show(5)

# 5b. West or East region with Discount > 0
discounted_we = df_transformed.filter(
    (F.col("Region").isin("West","East")) & (F.col("Discount_Pct") > 0)
)
print(f"  [4b] West/East + discounted    : {discounted_we.count():,} rows")
discounted_we.select("Order_ID","Customer_Name","Region","Category","Sales","Discount_Pct","Profit").show(5)

# 5c. Corporate segment, profitable orders (profit > 0)
corp_profit = df_transformed.filter(
    (F.col("Segment") == "Corporate") & (F.col("Profit") > 0)
)
print(f"  [4c] Corporate + profitable    : {corp_profit.count():,} rows")
corp_profit.select("Customer_Name","Segment","Category","Sales","Profit","Profit_Margin_Pct").show(5)

# 5d. Orders shipped in more than 5 days
slow_ship = df_transformed.filter(F.col("Days_to_Ship") > 5)
print(f"  [4d] Slow shipments (>5 days)  : {slow_ship.count():,} rows")
slow_ship.select("Order_ID","Ship_Mode","State","Days_to_Ship","Sales").show(5)

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 6 : AGGREGATION FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────
print("\n[SECTION 5] AGGREGATION FUNCTIONS (count, sum, avg, min, max)")
print("-" * 70)

overall = df_transformed.agg(
    F.count("Order_ID")                 .alias("Total_Orders"),
    F.countDistinct("Order_ID")         .alias("Unique_Orders"),
    F.countDistinct("Customer_ID")      .alias("Unique_Customers"),
    F.round(F.sum("Sales"),2)           .alias("Total_Sales"),
    F.round(F.avg("Sales"),2)           .alias("Avg_Sale_Per_Row"),
    F.round(F.min("Sales"),2)           .alias("Min_Sale"),
    F.round(F.max("Sales"),2)           .alias("Max_Sale"),
    F.round(F.sum("Profit"),2)          .alias("Total_Profit"),
    F.round(F.avg("Profit_Margin_Pct"),2).alias("Avg_Profit_Margin_Pct"),
    F.round(F.avg("Days_to_Ship"),2)    .alias("Avg_Days_to_Ship"),
)
print("\n  Overall Dataset Metrics:")
overall.show(truncate=False)

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 7 : GROUP BY AGGREGATIONS
# ─────────────────────────────────────────────────────────────────────────────
print("\n[SECTION 6] GROUP BY AGGREGATIONS")
print("-" * 70)

# 7a. By Category
by_cat = (
    df_transformed.groupBy("Category")
    .agg(
        F.count("*")                         .alias("Orders"),
        F.round(F.sum("Sales"),2)            .alias("Total_Sales"),
        F.round(F.avg("Sales"),2)            .alias("Avg_Sales"),
        F.round(F.sum("Profit"),2)           .alias("Total_Profit"),
        F.round(F.avg("Profit_Margin_Pct"),2).alias("Avg_Margin_Pct"),
        F.round(F.sum("Quantity"),0)         .alias("Units_Sold"),
    )
    .orderBy(F.desc("Total_Sales"))
)
print("\n  [6a] By Category:")
by_cat.show()

# 7b. By Region
by_region = (
    df_transformed.groupBy("Region")
    .agg(
        F.count("*")                         .alias("Orders"),
        F.round(F.sum("Sales"),2)            .alias("Total_Sales"),
        F.round(F.avg("Sales"),2)            .alias("Avg_Sales"),
        F.round(F.sum("Profit"),2)           .alias("Total_Profit"),
        F.countDistinct("Customer_ID")       .alias("Unique_Customers"),
    )
    .orderBy(F.desc("Total_Sales"))
)
print("  [6b] By Region:")
by_region.show()

# 7c. By Segment
by_seg = (
    df_transformed.groupBy("Segment")
    .agg(
        F.count("*")                         .alias("Orders"),
        F.round(F.sum("Sales"),2)            .alias("Total_Sales"),
        F.round(F.avg("Sales"),2)            .alias("Avg_Sales"),
        F.round(F.sum("Profit"),2)           .alias("Total_Profit"),
    )
    .orderBy(F.desc("Total_Sales"))
)
print("  [6c] By Segment:")
by_seg.show()

# 7d. By Category × Sub-Category (wide transformation — involves shuffle)
by_cat_sub = (
    df_transformed.groupBy("Category","Sub_Category")
    .agg(
        F.count("*")                         .alias("Orders"),
        F.round(F.sum("Sales"),2)            .alias("Total_Sales"),
        F.round(F.avg("Profit_Margin_Pct"),2).alias("Avg_Margin_Pct"),
    )
    .orderBy("Category", F.desc("Total_Sales"))
)
print("  [6d] By Category × Sub-Category (Wide Transformation / Shuffle):")
by_cat_sub.show(20)

# 7e. By Year & Quarter
by_year_qtr = (
    df_transformed.groupBy("Order_Year","Order_Quarter")
    .agg(
        F.count("*")                .alias("Orders"),
        F.round(F.sum("Sales"),2)   .alias("Total_Sales"),
        F.round(F.sum("Profit"),2)  .alias("Total_Profit"),
    )
    .orderBy("Order_Year","Order_Quarter")
)
print("  [6e] Year × Quarter Sales Trend:")
by_year_qtr.show(20)

# 7f. By Ship Mode
by_ship = (
    df_transformed.groupBy("Ship_Mode")
    .agg(
        F.count("*")                         .alias("Orders"),
        F.round(F.avg("Days_to_Ship"),2)     .alias("Avg_Days_to_Ship"),
        F.round(F.sum("Sales"),2)            .alias("Total_Sales"),
    )
    .orderBy("Avg_Days_to_Ship")
)
print("  [6f] By Ship Mode (avg delivery speed):")
by_ship.show()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 8 : HAVING – Filter on Aggregated Results
# ─────────────────────────────────────────────────────────────────────────────
print("\n[SECTION 7] HAVING (Filter on Aggregated Results)")
print("-" * 70)

# 8a. States with Total_Sales > 50,000
top_states = (
    df_transformed.groupBy("State","Region")
    .agg(
        F.count("*")               .alias("Orders"),
        F.round(F.sum("Sales"),2)  .alias("Total_Sales"),
        F.round(F.sum("Profit"),2) .alias("Total_Profit"),
    )
    .filter(F.col("Total_Sales") > 50000)
    .orderBy(F.desc("Total_Sales"))
)
print(f"\n  [7a] States with Total_Sales > $50,000 : {top_states.count()} states")
top_states.show()

# 8b. Sub-Categories with avg margin > 20%
high_margin_sub = (
    df_transformed.groupBy("Sub_Category","Category")
    .agg(
        F.count("*")                         .alias("Orders"),
        F.round(F.avg("Profit_Margin_Pct"),2).alias("Avg_Margin_Pct"),
        F.round(F.sum("Sales"),2)            .alias("Total_Sales"),
    )
    .filter(F.col("Avg_Margin_Pct") > 20)
    .orderBy(F.desc("Avg_Margin_Pct"))
)
print(f"  [7b] Sub-Categories with Avg Margin > 20% : {high_margin_sub.count()}")
high_margin_sub.show()

# 8c. Ship modes with more than 500 orders and avg ship > 3 days
slow_popular = (
    df_transformed.groupBy("Ship_Mode")
    .agg(
        F.count("*")                    .alias("Orders"),
        F.round(F.avg("Days_to_Ship"),2).alias("Avg_Days"),
    )
    .filter((F.col("Orders") > 500) & (F.col("Avg_Days") > 3))
    .orderBy(F.desc("Avg_Days"))
)
print(f"  [7c] Ship modes (>500 orders AND avg >3 days): {slow_popular.count()}")
slow_popular.show()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 9 : ADVANCED – Derived Columns & Bucketing
# ─────────────────────────────────────────────────────────────────────────────
print("\n[SECTION 8] ADVANCED TRANSFORMATIONS")
print("-" * 70)

df_advanced = (
    df_transformed
    # Sales tier
    .withColumn("Sales_Tier",
        F.when(F.col("Sales") < 100,  "Low (<$100)")
         .when(F.col("Sales") < 500,  "Medium ($100-500)")
         .when(F.col("Sales") < 2000, "High ($500-2K)")
         .otherwise("Premium (>$2K)"))
    # Profit status
    .withColumn("Profit_Status",
        F.when(F.col("Profit") > 0,  "Profit")
         .when(F.col("Profit") == 0, "Break-even")
         .otherwise("Loss"))
    # Discount bucket
    .withColumn("Discount_Band",
        F.when(F.col("Discount_Pct") == 0,  "No Discount")
         .when(F.col("Discount_Pct") <= 20, "Low (1-20%)")
         .when(F.col("Discount_Pct") <= 40, "Medium (21-40%)")
         .otherwise("High (>40%)"))
)

print("\n  [8a] Sales Tier distribution:")
(
    df_advanced.groupBy("Sales_Tier")
    .agg(
        F.count("*")              .alias("Orders"),
        F.round(F.sum("Sales"),2) .alias("Total_Sales"),
        F.round(F.avg("Profit"),2).alias("Avg_Profit"),
    )
    .orderBy("Sales_Tier")
    .show()
)

print("  [8b] Profit Status × Category:")
(
    df_advanced.groupBy("Category","Profit_Status")
    .agg(F.count("*").alias("Orders"))
    .orderBy("Category","Profit_Status")
    .show()
)

print("  [8c] Discount Band impact on Profit Margin:")
(
    df_advanced.groupBy("Discount_Band")
    .agg(
        F.count("*")                         .alias("Orders"),
        F.round(F.avg("Profit_Margin_Pct"),2).alias("Avg_Margin_Pct"),
        F.round(F.sum("Profit"),2)           .alias("Total_Profit"),
    )
    .orderBy("Discount_Band")
    .show()
)

print("  [8d] Top 10 Most Profitable Sub-Categories:")
(
    df_advanced.groupBy("Sub_Category","Category")
    .agg(
        F.round(F.sum("Profit"),2)           .alias("Total_Profit"),
        F.round(F.avg("Profit_Margin_Pct"),2).alias("Avg_Margin"),
        F.count("*")                         .alias("Orders"),
    )
    .orderBy(F.desc("Total_Profit"))
    .show(10)
)

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 10 : SPARK SQL
# ─────────────────────────────────────────────────────────────────────────────
print("\n[SECTION 9] SPARK SQL (createOrReplaceTempView)")
print("-" * 70)

df_advanced.createOrReplaceTempView("superstore")

print("\n  [SQL-1] Top 10 States by Revenue:")
spark.sql("""
    SELECT State, Region,
           COUNT(*) AS Orders,
           ROUND(SUM(Sales),2) AS Total_Sales,
           ROUND(SUM(Profit),2) AS Total_Profit
    FROM superstore
    GROUP BY State, Region
    ORDER BY Total_Sales DESC
    LIMIT 10
""").show()

print("  [SQL-2] Year-over-Year Sales Growth:")
spark.sql("""
    SELECT Order_Year,
           ROUND(SUM(Sales),2) AS Total_Sales,
           ROUND(SUM(Profit),2) AS Total_Profit,
           ROUND(AVG(Profit_Margin_Pct),2) AS Avg_Margin
    FROM superstore
    GROUP BY Order_Year
    ORDER BY Order_Year
""").show()

print("  [SQL-3] Region × Category profitability with RANK:")
spark.sql("""
    SELECT Region, Category,
           ROUND(SUM(Sales),2) AS Total_Sales,
           ROUND(SUM(Profit),2) AS Total_Profit,
           ROUND(AVG(Profit_Margin_Pct),2) AS Avg_Margin,
           RANK() OVER (PARTITION BY Region ORDER BY SUM(Sales) DESC) AS Rank_in_Region
    FROM superstore
    GROUP BY Region, Category
    ORDER BY Region, Rank_in_Region
""").show(20)

print("  [SQL-4] Customers with more than 15 orders (HAVING):")
spark.sql("""
    SELECT Customer_Name, Segment,
           COUNT(*) AS Total_Orders,
           ROUND(SUM(Sales),2) AS Total_Spend,
           ROUND(AVG(Profit_Margin_Pct),2) AS Avg_Margin
    FROM superstore
    GROUP BY Customer_Name, Segment
    HAVING COUNT(*) > 15
    ORDER BY Total_Orders DESC
    LIMIT 15
""").show()

print("  [SQL-5] Loss-making orders (Profit < 0) by Sub-Category:")
spark.sql("""
    SELECT Sub_Category, Category,
           COUNT(*) AS Loss_Orders,
           ROUND(SUM(Profit),2) AS Total_Loss,
           ROUND(AVG(Discount_Pct),1) AS Avg_Discount_Pct
    FROM superstore
    WHERE Profit < 0
    GROUP BY Sub_Category, Category
    ORDER BY Total_Loss ASC
    LIMIT 10
""").show()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 11 : PIPELINE SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
final_count = df_advanced.count()
print("\n" + "=" * 70)
print("  END-TO-END PIPELINE SUMMARY")
print("=" * 70)
print(f"""
  ┌─────────────────────────────────────────────────────────┐
  │  STEP 1  │ Ingestion       │ {raw_df.count():>6,} rows  │ 21 cols  │
  │  STEP 2  │ Deduplication   │ {after_dedup:>6,} rows  │          │
  │  STEP 3  │ Null Removal    │ {df_no_nulls.count():>6,} rows  │          │
  │  STEP 4  │ String Cleaning │ {df_clean.count():>6,} rows  │          │
  │  STEP 5  │ Transformation  │ {df_transformed.count():>6,} rows  │ +8 cols  │
  │  STEP 6  │ Feature Engg.   │ {final_count:>6,} rows  │ +3 cols  │
  └─────────────────────────────────────────────────────────┘

  Transformations Applied:
    ✓ Order_Date / Ship_Date  : String → DateType (dd/MM/yyyy)
    ✓ Days_to_Ship            : Derived via datediff()
    ✓ Order_Year/Month/Quarter: Extracted from date
    ✓ Revenue_Per_Unit        : Sales / Quantity
    ✓ Profit_Margin_Pct       : (Profit/Sales)*100
    ✓ Discount_Pct            : Discount*100 as Integer
    ✓ Sales_Tier              : Bucketed ($100/$500/$2K)
    ✓ Profit_Status           : Profit / Break-even / Loss
    ✓ Discount_Band           : No/Low/Medium/High

  Aggregations Run:
    ✓ Overall metrics (count, sum, avg, min, max)
    ✓ By Category, Region, Segment, Ship Mode
    ✓ By Category × Sub-Category  [Wide Transformation + Shuffle]
    ✓ By Year × Quarter
    ✓ HAVING: States >$50K, Margin >20%, Slow shippers
    ✓ Spark SQL: Top states, YoY growth, RANK(), HAVING, Loss orders
""")

spark.stop()
print("  Spark session stopped. Pipeline complete ✓")
print("=" * 70)
