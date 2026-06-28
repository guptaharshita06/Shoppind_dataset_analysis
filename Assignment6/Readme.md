# Week 6 — Spark Architecture & Efficient Data Processing
**Celebal Technologies | Data Engineering Internship**

> **Name:** Harshita Gupta
> **College:** JECRC Foundation, Jaipur
> **Dataset:** Superstore Sales Data
> **Tools:** PySpark · Google Colab · Python 3.12

---

## 📌 Overview

This assignment demonstrates hands-on proficiency with Apache Spark for large-scale data processing. It covers Spark architecture fundamentals, lazy evaluation, DataFrame transformations, file format performance comparison, and end-to-end pipeline construction — all implemented on the Superstore Sales dataset.

---

## 📚 Topics Covered

| # | Topic |
|---|---|
| Q1 | Spark Architecture — Driver, Cluster Manager, Executor |
| Q2 | Lazy Evaluation & DAG (Directed Acyclic Graph) |
| Q3 | Reading CSV files with header and inferSchema |
| Q4 | CSV vs Parquet — storage format and performance |
| Q5 | Filtering and selecting required columns |
| Q6 | Renaming columns and casting data types |
| Q7 | Lineage Graph (DAG) and fault tolerance |
| Q8 | Multi-condition filtering with AND |
| Q9 | Predicate Pushdown in Parquet |
| Q10 | Adding derived columns (18% tax) |
| Q11 | Transformations vs Actions with examples |
| Q12 | Parquet → filter nulls → save as CSV pipeline |
| Q13 | Client Mode vs Cluster Mode |
| Q14 | Multi-condition filtering with OR |
| Q15 | Why show() is safer than collect() on large data |
| ⭐ Bonus | End-to-End Data Pipeline |

---

## 🗂️ Project Structure

```
spark_assignment/
├── Week6_Spark_Assignment.ipynb   ← Main notebook (submit this)
├── superstore.csv                 ← Input dataset
└── output/
    ├── superstore_csv/            ← Q4: CSV output
    ├── superstore_parquet/        ← Q4: Parquet output
    ├── q12_output_csv/            ← Q12: Null-filtered CSV
    └── final_pipeline/            ← Bonus: Partitioned Parquet (by Category)
```

---

## ⚙️ Setup & Installation

### Google Colab (Recommended)
```python
# Cell 1 — Install PySpark
!pip install pyspark -q
print("Done!")
```

### Local Machine
```bash
pip install pyspark findspark
python -c "import pyspark; print(pyspark.__version__)"
java -version  # Java 11+ required
```

### ⚠️ Windows Fix — Multiple Java Versions
If PySpark hangs on Windows, add this as the very first cell in your notebook:
```python
import os
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.31.11-hotspot"
os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]
```

---

## 🚀 How to Run

### Google Colab
1. Go to [colab.research.google.com](https://colab.research.google.com)
2. Upload `Week6_Spark_Assignment.ipynb` via **File → Upload notebook**
3. Upload `superstore.csv` via the **Files panel** (folder icon on left sidebar)
4. Run all cells: **Runtime → Run All** (or `Ctrl + F9`)

### Local Jupyter
```bash
cd spark_assignment
jupyter notebook
# Open Week6_Spark_Assignment.ipynb → Cell → Run All
```

### Jupyter Shortcuts
| Shortcut | Action |
|---|---|
| `Shift + Enter` | Run current cell and go to next |
| `Ctrl + Enter` | Run current cell and stay |
| `Ctrl + F9` (Colab) | Run all cells |
| `Kernel → Restart & Run All` | Full restart and re-run |

---

## 📊 Dataset

| Property | Details |
|---|---|
| Name | Superstore Sales Dataset |
| Source | [Kaggle — vivek468/superstore-dataset-final](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final) |
| File | `Sample - Superstore.csv` → rename to `superstore.csv` |
| Rows | ~9,994 |
| Columns | 21 |
| Key Fields | Order ID, Category, Region, Sales, Quantity, Discount, Profit |

---

## 🏗️ Spark Architecture (Q1)

```
┌─────────────────────────────────────────┐
│           DRIVER PROGRAM                │  ← SparkSession lives here
│  Builds DAG → Schedules tasks           │
└─────────────────┬───────────────────────┘
                  │ submits tasks
         ┌────────▼────────┐
         │ CLUSTER MANAGER │  ← YARN / Standalone / Kubernetes
         └────────┬────────┘
      ┌───────────┼───────────┐
      ▼           ▼           ▼
 ┌─────────┐ ┌─────────┐ ┌─────────┐
 │Executor │ │Executor │ │Executor │  ← Run tasks in parallel
 └─────────┘ └─────────┘ └─────────┘
```

| Component | Role |
|---|---|
| **Driver** | Entry point. Creates SparkSession, builds the DAG, schedules tasks |
| **Cluster Manager** | Resource broker (YARN/Standalone/K8s). Allocates CPU & memory |
| **Executor** | JVM on worker nodes. Executes tasks, stores partitions in memory/disk |

---

## 💡 Key Concepts

### Lazy Evaluation (Q2)
Spark does NOT execute transformations immediately. It builds a logical plan (DAG) and only executes when an **action** is called.
```
df.filter(...)       → NO execution (adds to plan)
  .select(...)       → NO execution (adds to plan)
  .withColumn(...)   → NO execution (adds to plan)
  .count()  ← ACTION → NOW Spark executes the full DAG ✅
```

### Transformations vs Actions (Q11)
| Type | Behaviour | Examples |
|---|---|---|
| **Transformation** | Lazy — builds DAG only | `filter()`, `select()`, `withColumn()`, `groupBy()` |
| **Action** | Eager — triggers execution | `count()`, `show()`, `collect()`, `write()` |

### CSV vs Parquet (Q4)
| Feature | CSV | Parquet |
|---|---|---|
| Storage | Row-based (text) | Columnar (binary) |
| File Size | Large | 3–10x smaller |
| Schema | Not embedded | Embedded in footer |
| Column Pruning | ❌ Reads all columns | ✅ Reads only needed columns |
| Predicate Pushdown | ❌ Not supported | ✅ Skips row-groups |
| Best For | Data exchange | Analytics pipelines |

### Client Mode vs Cluster Mode (Q13)
| Aspect | Client Mode | Cluster Mode |
|---|---|---|
| Driver Location | Runs on submitting machine | Runs inside the cluster |
| Logs | Printed to your terminal | Stored in cluster (use Spark UI) |
| Network | Driver ↔ Executors cross-network | Co-located — low latency |
| Best For | Development, notebooks | Production pipelines |

---

## 📝 Code Snippets

### SparkSession Setup
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = (
    SparkSession.builder
    .appName("Week6_SparkAssignment")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
```

### Q3 — Read CSV
```python
df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("superstore.csv"))
```

### Q5 — Filter + Select
```python
result = (df
          .filter(F.col("Category") == "Technology")
          .select("Product_ID", "Sales"))
```

### Q6 — Rename + Cast
```python
df_revised = (df
              .withColumnRenamed("Sub_Category", "sub_category")
              .withColumn("Sales", F.col("Sales").cast(DoubleType())))
```

### Q8 — AND Filter
```python
df_filtered = df.filter(
    (F.col("Ship_Mode") == "First Class") &
    (F.col("Sales") > 1000)
)
```

### Q10 — Add Derived Column
```python
df_tax = df.withColumn(
    "final_price",
    F.round(F.col("base_price") * 1.18, 2)
)
```

### Q12 — Parquet → Filter Nulls → CSV
```python
df_clean = (spark.read
            .parquet("path/to/input")
            .filter(F.col("user_id").isNotNull()))

df_clean.write.mode("overwrite").option("header", "true").csv("path/to/output")
```

### Q14 — OR Filter
```python
df_filtered = df.filter(
    (F.col("Region") == "West") |
    (F.col("Segment") == "Corporate")
)
```

---

## ✅ Best Practices

| ✅ Do This | ❌ Avoid This |
|---|---|
| Use `show(n)` to preview data | `collect()` on large data — causes OOM crash |
| Define explicit `StructType` schema | `inferSchema` on large files — double scan |
| Write output as Parquet | CSV for analytics — slow and large |
| `filter()` early in the pipeline | `filter()` after joins — wastes shuffle |
| `cache()` DataFrames used multiple times | Recomputing the same expensive transformations |
| Set `shuffle.partitions = 4` for local | Default 200 partitions on local machine |

---

## 🗃️ Output Files

| File / Folder | Format | Description |
|---|---|---|
| `output/superstore_csv/` | CSV | Full processed dataset (Q4) |
| `output/superstore_parquet/` | Parquet | Full processed dataset (Q4) |
| `output/q12_output_csv/` | CSV | Null-filtered output (Q12) |
| `output/final_pipeline/` | Parquet | Bonus — partitioned by Category |

---

## 🔧 Troubleshooting

| Issue | Cause | Fix |
|---|---|---|
| Cell stuck at `[*]` forever | Java path conflict | Set `JAVA_HOME` explicitly at top of notebook |
| `No module named pyspark` | PySpark not installed | Run `!pip install pyspark` in notebook |
| `No module named findspark` | findspark not installed | Run `!pip install findspark` |
| `PATH_NOT_FOUND superstore.csv` | File not uploaded to Colab | Upload via Files panel on left sidebar |
| `AnalysisException: column not found` | Column name mismatch | Run `df.columns` to check exact names |
| `OutOfMemoryError` | `collect()` on large data | Replace with `show(n)` or `write()` |

---

## 📎 References

- [Apache Spark Docs](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [Superstore Dataset — Kaggle](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final)
- [Google Colab](https://colab.research.google.com)
- Celebal Technologies LMS — Week 6 Assignment Guide

---

*Harshita Gupta · Data Engineering Intern · Celebal Technologies · 2025*
