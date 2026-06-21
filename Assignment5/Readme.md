# 🔥 Apache Spark Assignment — Superstore Dataset
**Celebal Technologies Internship | Week 5**  
**Author:** Harshita  
**Topic:** Data Cleaning, Transformation & Aggregation using PySpark

---

## 📌 Objective

Understand Apache Spark fundamentals and build an end-to-end data processing pipeline using PySpark DataFrames on the Superstore Sales Dataset covering:

- Limitations of MapReduce vs advantages of Spark (in-memory processing)
- DataFrame concepts and immutability
- Data cleaning — duplicates, nulls, invalid values
- Filtering on age range, category, region
- Aggregation functions — `count`, `sum`, `avg`, `min`, `max`
- GroupBy with HAVING conditions
- Wide transformations and shuffle operations
- Schema modifications — casting, renaming columns
- Complete pipeline combining cleaning + aggregation
- Spark SQL via `createOrReplaceTempView`

---

## 📂 Project Structure

```
spark_assignment/
├── Sample - Superstore.csv          ← Input dataset (download from Kaggle)
├── spark_superstore_assignment.py   ← Main PySpark script
└── README.md                        ← This file
```

---

## 📥 Dataset

| Property | Details |
|---|---|
| **Name** | Sample - Superstore |
| **Source** | [Kaggle — vivek468/superstore-dataset-final](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final) |
| **Rows** | 9,994 |
| **Columns** | 21 |
| **Format** | CSV |

### Columns in Dataset

| Column | Type | Description |
|---|---|---|
| Row ID | Integer | Unique row identifier |
| Order ID | String | Unique order identifier |
| Order Date | String (DD/MM/YYYY) | Date order was placed |
| Ship Date | String (DD/MM/YYYY) | Date order was shipped |
| Ship Mode | String | Shipping method |
| Customer ID | String | Unique customer identifier |
| Customer Name | String | Full name of customer |
| Segment | String | Consumer / Corporate / Home Office |
| Country | String | Always "United States" |
| City | String | City of delivery |
| State | String | State of delivery |
| Postal Code | String | Postal code |
| Region | String | East / West / Central / South |
| Product ID | String | Unique product identifier |
| Category | String | Technology / Furniture / Office Supplies |
| Sub-Category | String | Product sub-category (17 types) |
| Product Name | String | Full product name |
| Sales | Double | Total sale amount ($) |
| Quantity | Integer | Units ordered |
| Discount | Double | Discount applied (0.0 to 1.0) |
| Profit | Double | Profit earned ($) |

---

## ⚙️ Prerequisites

### 1. Python
```bash
python --version   # Python 3.8+ required
```

### 2. Java (required by Spark)
```bash
java -version      # Java 11 recommended
```
If not installed → Download from [https://adoptium.net](https://adoptium.net)

### 3. PySpark
```bash
pip install pyspark
```

---

## 🚀 How to Run

### Step 1 — Download the Dataset
Go to: https://www.kaggle.com/datasets/vivek468/superstore-dataset-final  
Download the ZIP → Extract → You will get `Sample - Superstore.csv`

### Step 2 — Place Files Together
```
spark_assignment/
├── Sample - Superstore.csv
└── spark_superstore_assignment.py
```

### Step 3 — Update CSV Path (if needed)
Open `spark_superstore_assignment.py` and find line ~47:
```python
# Change this to your actual file path
CSV_PATH = "Sample - Superstore.csv"          # if in same folder

# OR full path (Windows):
CSV_PATH = r"C:\Users\YourName\Downloads\Sample - Superstore.csv"
```

### Step 4 — Run the Script
```bash
python spark_superstore_assignment.py
```

---

## 📊 What the Script Does (Pipeline Sections)

### Section 1 — Load Dataset
- Reads CSV with explicit schema (`StructType`)
- Prints row count, column count, schema, and sample rows

**Input:** `Sample - Superstore.csv`  
**Output:** Raw DataFrame with 9,994 rows × 21 columns

---

### Section 2 — Data Cleaning
| Step | Operation | Method Used |
|---|---|---|
| 2a | Null audit | `select + count(when(isNull))` |
| 2b | Remove exact duplicates | `dropDuplicates()` |
| 2c | Remove Order+Product duplicates | `dropDuplicates(["Order_ID","Product_ID"])` |
| 2d | Drop rows with critical NULLs | `dropna(subset=[...])` |
| 2e | Fix empty strings in Region/Segment | `when(col == "").otherwise(...)` |
| 2e | Remove negative/zero Sales | `filter(col("Sales") > 0)` |
| 2f | Final null audit | Verify 0 nulls in critical columns |

**Output:** Clean DataFrame — 9,994 rows (this dataset was already clean)

---

### Section 3 — Schema Transformation
| Transformation | Details |
|---|---|
| `Order_Date` cast | String → `DateType` using `to_date("dd/MM/yyyy")` |
| `Ship_Date` cast | String → `DateType` |
| New: `Order_Year`, `Order_Month`, `Order_Quarter` | Extracted from date |
| New: `Days_to_Ship` | `datediff(Ship_Date, Order_Date)` |
| New: `Discount_Pct` | `Discount × 100` as Integer |
| New: `Revenue_Per_Unit` | `Sales / Quantity` |
| New: `Profit_Margin_Pct` | `(Profit / Sales) × 100` |

**Output:** Transformed DataFrame with 29 columns

---

### Section 4 — Filtering
| Filter | Condition |
|---|---|
| 4a | Technology + Sales > $1,000 → **3,144 rows** |
| 4b | West/East region + Discount > 0 → **3,523 rows** |
| 4c | Corporate segment + Profit > 0 → **3,108 rows** |
| 4d | Days to ship > 5 → **2,838 rows** |

---

### Section 5 — Aggregation Functions

Overall dataset metrics:

| Metric | Value |
|---|---|
| Total Orders | 9,994 |
| Unique Orders | 9,865 |
| Unique Customers | 9,473 |
| Total Sales | ~$68.4M |
| Avg Sale Per Row | $6,848.97 |
| Total Profit | ~$12.3M |
| Avg Profit Margin | 17.53% |
| Avg Days to Ship | 3.97 days |

---

### Section 6 — GroupBy Aggregations

**By Category:**
| Category | Orders | Total Sales | Avg Margin |
|---|---|---|---|
| Technology | 3,315 | $41.8M | 22.45% |
| Furniture | 3,335 | $21.5M | 7.65% |
| Office Supplies | 3,344 | $5.2M | 22.52% |

**By Region:**
| Region | Orders | Total Sales | Total Profit |
|---|---|---|---|
| West | 2,520 | $17.5M | $3.16M |
| East | 2,520 | $17.5M | $3.11M |
| Central | 2,447 | $16.9M | $3.09M |
| South | 2,507 | $16.6M | $2.91M |

Also grouped by: **Segment**, **Category × Sub-Category** *(wide transformation — shuffle)*, **Year × Quarter**, **Ship Mode**

---

### Section 7 — HAVING (Filter on Aggregated Results)
| Query | Result |
|---|---|
| States with Total Sales > $50,000 | 32 states |
| Sub-categories with Avg Margin > 20% | 13 sub-categories |
| Ship modes with >500 orders AND avg >3 days | All 4 modes |

---

### Section 8 — Advanced Derived Columns
| Column | Logic |
|---|---|
| `Sales_Tier` | Low / Medium / High / Premium buckets |
| `Profit_Status` | Profit / Break-even / Loss |
| `Discount_Band` | No / Low / Medium / High |

**Key finding:** Furniture has **946 loss-making orders** — highest among all categories.

---

### Section 9 — Spark SQL

Queries run via `createOrReplaceTempView("superstore")`:

| Query | Description |
|---|---|
| SQL-1 | Top 10 states by revenue |
| SQL-2 | Year-over-Year sales (2014–2017) |
| SQL-3 | Region × Category with `RANK() OVER (PARTITION BY ...)` |
| SQL-4 | High-frequency customers using `HAVING COUNT(*) > 15` |
| SQL-5 | Loss-making sub-categories where `Profit < 0` |

---

## 💡 Key Insights

**1. MapReduce vs Spark**  
Spark processes data **in-memory**, avoiding repeated disk I/O. For multi-step pipelines like this one, Spark can be 10–100× faster than MapReduce.

**2. Immutability**  
Every `withColumn()`, `filter()`, or `groupBy()` creates a **new DataFrame** — the original is never modified. This enables Spark's lazy evaluation and fault tolerance.

**3. Narrow vs Wide Transformations**  
- `filter()`, `withColumn()` → **Narrow** — no data movement between partitions  
- `groupBy()`, `join()` → **Wide** — causes a **shuffle** across partitions (expensive)  
- `spark.sql.shuffle.partitions = 8` was set to match dataset size

**4. Discount kills Furniture profit**  
Loss-making orders in Furniture (Bookcases: -$93K, Furnishings: -$87K) all have avg discounts of 15–19%, confirming high discounts directly cause losses.

**5. Technology is the star category**  
Despite fewer units sold, Technology generates $41.8M (61% of total revenue) with a healthy 22.45% profit margin.

---

## 📤 Output

All output is printed to terminal — section-wise tables showing:
- Schema before and after transformation
- Cleaning audit results
- Filtered DataFrames (row counts + sample rows)
- Aggregation tables (category, region, segment, year-quarter, ship mode)
- HAVING filter results
- Spark SQL query results

---

## 🛠️ Tech Stack

| Tool | Version | Purpose |
|---|---|---|
| Python | 3.8+ | Runtime |
| PySpark | 4.x | Distributed data processing |
| Apache Spark | 4.1.2 | Underlying engine |
| Java | 11+ | Spark dependency |
| CSV | — | Input format |
