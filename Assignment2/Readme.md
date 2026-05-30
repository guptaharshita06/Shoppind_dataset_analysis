# 🛒 Superstore Sales Analysis — SQL

## 📌 Objective
Analyze Superstore sales data using MySQL Workbench to extract
business insights through SQL filtering, aggregation, and trend analysis.

This is a **Week 2 Assignment** of the Celebal Technologies Internship Program.

---

## 📂 Dataset
- **Source:** [Kaggle — Superstore Dataset](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final)
- **Rows:** 9,994 | **Columns:** 21
- **Period:** January 2014 – December 2017
- **Regions:** East, West, South, Central

---

## 🛠️ Tools Used
- MySQL 9.0
- MySQL Workbench
- Python (pandas + mysql-connector) — for CSV import

---

## 📁 Repository Structure
```
superstore-sql-analysis/
│
├── superstore_analysis.sql     ← Main SQL script
├── README.md
│
├── data/
│   └── Sample - Superstore.csv
│
└── results/
    ├── region_sales.csv
    ├── top_products.csv
    └── monthly_trend.csv
```

---

## 📊 Analysis Performed

| Step | Task | Description |
|------|------|-------------|
| 1 | Setup | Created database, table, imported CSV |
| 2 | Explore | Schema review, sample data, NULL checks |
| 3 | WHERE Filters | Filter by region, category, date, sales |
| 4 | GROUP BY | Aggregations by region, category, segment |
| 5 | Sort & Limit | TOP 10 products, cities, sub-categories |
| 6 | Business Cases | Monthly trends, top customers, discount impact |
| 7 | Validation | Row counts, NULL audit, data quality checks |

---

## 💡 Key Insights

- 🌍 **West region** generates the highest total sales (~32% of revenue)
- 💻 **Technology** is the most profitable category
- 📉 **Discounts above 20%** consistently result in net losses
- 📅 **Q4 (Oct–Dec)** is the peak sales quarter every year
- 🖨️ **Copiers & Phones** are top revenue sub-categories
- 👥 **Consumer segment** places the most orders
- 🚚 **Standard Class** is the most used shipping mode

---

## 🚀 How to Run

1. Clone this repository
2. Import CSV using the Python script:
   ```bash
   pip install pandas mysql-connector-python
   python import.py
   ```
3. Open `superstore_analysis.sql` in MySQL Workbench
4. Run queries section by section (Step 2 → Step 7)

---

## 👩‍💻 Author
**Harshita Gupta**
Celebal Technologies — Data Engineering Internship
