# Superstore Sales Analysis — SQL Project

## Objective
Analyze sales data using SQL by applying Subqueries, CTEs, and Window Functions to solve business queries.

---

## Dataset
- **Name:** Superstore Sales Dataset
- **Source:** Kaggle (vivek468/superstore-dataset-final)
- **Rows:** 9,994 | **Columns:** 21
- **Period:** 2015 – 2019

---

## Tools Used
- MySQL 8.0 + MySQL Workbench
- Python (pandas, sqlalchemy, pymysql)

---

## Steps Performed

**1. Data Loading**
- Imported Sample - Superstore.csv into MySQL using Python
- Loaded into superstore_raw table

**2. Table Creation**
- Created 3 normalised tables from superstore_raw:
  - customers (60 unique records)
  - products (9,863 unique records)
  - orders (9,994 records)

**3. Subqueries**
- Scalar Subquery: Filtered orders above average sales value
- Correlated Subquery: Found highest order per customer

**4. CTEs**
- Calculated total sales, profit, and order count per customer

**5. Window Functions**
- ROW_NUMBER() — unique sequential ranking
- RANK() — ranking with gaps after ties
- DENSE_RANK() — ranking without gaps
- PARTITION BY Region — region-wise ranking
- SUM() OVER() — running total per customer

**6. Business Queries Solved**
- Top 5 customers by revenue
- Bottom 5 customers by revenue
- Single-order customers
- Above-average sales orders
- Region-wise performance summary
- Category profitability ranking

---

## Key Insights
- Technology is the top category in both revenue and profit
- West region leads in total sales
- East region has the best profit margin (5.21%)
- South region has the lowest margin — high discounts
- Top 5 customers contribute ~10% of total revenue
- Orders with 40-50% discount often result in negative profit

---

## Files
- superstore_analysis.sql — all SQL queries
- superstore_notebook.html — visual output
- README.md — this file

---

## Author
Harshita
Celebal Technologies Internship
