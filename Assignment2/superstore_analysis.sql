CREATE DATABASE IF NOT EXISTS superstore_db;
USE superstore_db;

SELECT COUNT(*) FROM superstore;
SELECT * FROM superstore LIMIT 10;

DROP TABLE IF EXISTS superstore;

CREATE TABLE superstore (
    Row_ID        INT,
    Order_ID      VARCHAR(20),
    Order_Date    DATE,
    Ship_Date     DATE,
    Ship_Mode     VARCHAR(30),
    Customer_ID   VARCHAR(20),
    Customer_Name VARCHAR(100),
    Segment       VARCHAR(20),
    Country       VARCHAR(50),
    City          VARCHAR(50),
    State         VARCHAR(50),
    Postal_Code   VARCHAR(10),
    Region        VARCHAR(20),
    Product_ID    VARCHAR(20),
    Category      VARCHAR(30),
    Sub_Category  VARCHAR(30),
    Product_Name  VARCHAR(200),
    Sales         DECIMAL(10,4),
    Quantity      INT,
    Discount      DECIMAL(5,4),
    Profit        DECIMAL(10,4)
);

--  Total row count
SELECT COUNT(*) AS total_rows FROM superstore;

-- First 10 rows
SELECT * FROM superstore LIMIT 10;

--  Distinct values in key columns
SELECT DISTINCT Region     FROM superstore ORDER BY 1;
SELECT DISTINCT Category   FROM superstore ORDER BY 1;
SELECT DISTINCT Sub_Category FROM superstore ORDER BY 1;
SELECT DISTINCT Segment    FROM superstore ORDER BY 1;
SELECT DISTINCT Ship_Mode  FROM superstore ORDER BY 1;

--  Date range of orders
SELECT
    MIN(Order_Date) AS earliest_order,
    MAX(Order_Date) AS latest_order
FROM superstore;

--  Sales summary statistics
SELECT
    ROUND(MIN(Sales), 2)  AS min_sales,
    ROUND(MAX(Sales), 2)  AS max_sales,
    ROUND(AVG(Sales), 2)  AS avg_sales,
    ROUND(SUM(Sales), 2)  AS total_sales
FROM superstore;

--  Check for NULL values in critical columns
SELECT
    SUM(CASE WHEN Order_ID      IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN Customer_Name IS NULL THEN 1 ELSE 0 END) AS null_customer,
    SUM(CASE WHEN Sales         IS NULL THEN 1 ELSE 0 END) AS null_sales,
    SUM(CASE WHEN Profit        IS NULL THEN 1 ELSE 0 END) AS null_profit
FROM superstore;


--  Filter by Region — West only
SELECT * FROM superstore
WHERE Region = 'West'
LIMIT 20;

--  Filter by Category — Technology
SELECT * FROM superstore
WHERE Category = 'Technology'
LIMIT 20;

--  Filter by Date Range — Year 2017
SELECT * FROM superstore
WHERE Order_Date BETWEEN '2017-01-01' AND '2017-12-31'
LIMIT 20;

-- Filter by Sales threshold — orders > $500
SELECT Order_ID, Customer_Name, Product_Name, Sales
FROM superstore
WHERE Sales > 500
ORDER BY Sales DESC
LIMIT 20;

--  Filter by Profit — only profitable orders
SELECT Order_ID, Customer_Name, Product_Name, Sales, Profit
FROM superstore
WHERE Profit > 0
LIMIT 20;

--  Combined filter — West region, Technology, profitable
SELECT Order_ID, Customer_Name, Product_Name, Sales, Profit
FROM superstore
WHERE Region = 'West'
  AND Category = 'Technology'
  AND Profit > 0
ORDER BY Sales DESC;

-- Filter with LIKE — products containing "Phone"
SELECT Product_Name, Sales, Profit
FROM superstore
WHERE Product_Name LIKE '%Phone%'
ORDER BY Sales DESC;

--  Negative profit (loss-making orders)
SELECT Order_ID, Customer_Name, Product_Name, Sales, Profit
FROM superstore
WHERE Profit < 0
ORDER BY Profit ASC
LIMIT 20;

--  Total Sales & Profit by Region
SELECT
    Region,
    COUNT(*)            AS num_orders,
    ROUND(SUM(Sales),2)  AS total_sales,
    ROUND(SUM(Profit),2) AS total_profit,
    ROUND(AVG(Sales),2)  AS avg_sales
FROM superstore
GROUP BY Region
ORDER BY total_sales DESC;

--  Total Sales & Profit by Category
SELECT
    Category,
    COUNT(*)              AS num_orders,
    SUM(Quantity)         AS total_qty,
    ROUND(SUM(Sales),2)   AS total_sales,
    ROUND(SUM(Profit),2)  AS total_profit,
    ROUND(AVG(Discount),4) AS avg_discount
FROM superstore
GROUP BY Category
ORDER BY total_sales DESC;

--  Sales by Sub-Category
SELECT
    Sub_Category,
    COUNT(*)             AS num_orders,
    ROUND(SUM(Sales),2)  AS total_sales,
    ROUND(SUM(Profit),2) AS total_profit
FROM superstore
GROUP BY Sub_Category
ORDER BY total_sales DESC;

--  Sales by Segment
SELECT
    Segment,
    COUNT(DISTINCT Customer_ID) AS unique_customers,
    ROUND(SUM(Sales),2)          AS total_sales,
    ROUND(SUM(Profit),2)         AS total_profit
FROM superstore
GROUP BY Segment
ORDER BY total_sales DESC;

--  Sales by Ship Mode
SELECT
    Ship_Mode,
    COUNT(*)             AS num_orders,
    ROUND(SUM(Sales),2)  AS total_sales,
    ROUND(AVG(Sales),2)  AS avg_order_value
FROM superstore
GROUP BY Ship_Mode
ORDER BY num_orders DESC;

--  Average Discount by Category
SELECT
    Category,
    ROUND(AVG(Discount)*100, 2) AS avg_discount_pct
FROM superstore
GROUP BY Category;

--  Top 10 Products by Sales
SELECT
    Product_Name,
    Category,
    ROUND(SUM(Sales),2)  AS total_sales,
    ROUND(SUM(Profit),2) AS total_profit
FROM superstore
GROUP BY Product_Name, Category
ORDER BY total_sales DESC
LIMIT 10;

-- Top 10 Products by Profit
SELECT
    Product_Name,
    Category,
    ROUND(SUM(Profit),2) AS total_profit,
    ROUND(SUM(Sales),2)  AS total_sales
FROM superstore
GROUP BY Product_Name, Category
ORDER BY total_profit DESC
LIMIT 10;

--  Bottom 10 Products by Profit (biggest losses)
SELECT
    Product_Name,
    Category,
    ROUND(SUM(Profit),2) AS total_profit
FROM superstore
GROUP BY Product_Name, Category
ORDER BY total_profit ASC
LIMIT 10;

-- Top 5 Sub-Categories by Quantity Sold
SELECT
    Sub_Category,
    SUM(Quantity) AS total_quantity
FROM superstore
GROUP BY Sub_Category
ORDER BY total_quantity DESC
LIMIT 5;

--  Top 5 States by Sales
SELECT
    State,
    ROUND(SUM(Sales),2)  AS total_sales,
    ROUND(SUM(Profit),2) AS total_profit
FROM superstore
GROUP BY State
ORDER BY total_sales DESC
LIMIT 5;

-- Top 5 Cities by Sales
SELECT
    City,
    State,
    ROUND(SUM(Sales),2)  AS total_sales,
    ROUND(SUM(Profit),2) AS total_profit
FROM superstore
GROUP BY City, State
ORDER BY total_sales DESC
LIMIT 5;

-- MONTHLY SALES TREND
SELECT
    YEAR(Order_Date)  AS yr,
    MONTH(Order_Date) AS mo,
    DATE_FORMAT(Order_Date, '%Y-%m') AS month_year,
    ROUND(SUM(Sales),2)   AS monthly_sales,
    ROUND(SUM(Profit),2)  AS monthly_profit,
    COUNT(DISTINCT Order_ID) AS num_orders
FROM superstore
GROUP BY yr, mo, month_year
ORDER BY yr, mo;

-- YEARLY SALES TREND
SELECT
    YEAR(Order_Date) AS year,
    ROUND(SUM(Sales),2)   AS total_sales,
    ROUND(SUM(Profit),2)  AS total_profit,
    COUNT(DISTINCT Order_ID) AS total_orders,
    COUNT(DISTINCT Customer_ID) AS unique_customers
FROM superstore
GROUP BY year
ORDER BY year;

-- TOP 10 CUSTOMERS by Sales
SELECT
    Customer_ID,
    Customer_Name,
    Segment,
    COUNT(DISTINCT Order_ID)  AS total_orders,
    ROUND(SUM(Sales),2)        AS total_sales,
    ROUND(SUM(Profit),2)       AS total_profit
FROM superstore
GROUP BY Customer_ID, Customer_Name, Segment
ORDER BY total_sales DESC
LIMIT 10;

-- TOP 10 CUSTOMERS by Profit
SELECT
    Customer_Name,
    Segment,
    ROUND(SUM(Profit),2) AS total_profit,
    ROUND(SUM(Sales),2)  AS total_sales
FROM superstore
GROUP BY Customer_Name, Segment
ORDER BY total_profit DESC
LIMIT 10;

-- PROFIT MARGIN by Category
SELECT
    Category,
    ROUND(SUM(Sales),2)   AS total_sales,
    ROUND(SUM(Profit),2)  AS total_profit,
    ROUND(SUM(Profit)/SUM(Sales)*100, 2) AS profit_margin_pct
FROM superstore
GROUP BY Category
ORDER BY profit_margin_pct DESC;

--  PROFIT MARGIN by Sub-Category
SELECT
    Sub_Category,
    ROUND(SUM(Sales),2)  AS total_sales,
    ROUND(SUM(Profit),2) AS total_profit,
    ROUND(SUM(Profit)/SUM(Sales)*100, 2) AS profit_margin_pct
FROM superstore
GROUP BY Sub_Category
ORDER BY profit_margin_pct DESC;

--  DETECT DUPLICATE ORDER LINES

SELECT
    Order_ID,
    Product_ID,
    COUNT(*) AS occurrences
FROM superstore
GROUP BY Order_ID, Product_ID
HAVING occurrences > 1
ORDER BY occurrences DESC;

--  CUSTOMERS WITH LOSSES (net negative profit)
SELECT
    Customer_Name,
    ROUND(SUM(Sales),2)  AS total_sales,
    ROUND(SUM(Profit),2) AS total_profit
FROM superstore
GROUP BY Customer_Name
HAVING total_profit < 0
ORDER BY total_profit ASC;

--  IMPACT OF DISCOUNT ON PROFIT
SELECT
    CASE
        WHEN Discount = 0          THEN '0% — No Discount'
        WHEN Discount BETWEEN 0.01 AND 0.10 THEN '1–10%'
        WHEN Discount BETWEEN 0.11 AND 0.20 THEN '11–20%'
        WHEN Discount BETWEEN 0.21 AND 0.30 THEN '21–30%'
        ELSE 'Above 30%'
    END AS discount_bucket,
    COUNT(*) AS num_orders,
    ROUND(AVG(Profit),2) AS avg_profit,
    ROUND(SUM(Profit),2) AS total_profit
FROM superstore
GROUP BY discount_bucket
ORDER BY discount_bucket;

--  REGION + CATEGORY CROSS ANALYSIS
SELECT
    Region,
    Category,
    ROUND(SUM(Sales),2)  AS total_sales,
    ROUND(SUM(Profit),2) AS total_profit
FROM superstore
GROUP BY Region, Category
ORDER BY Region, total_sales DESC;

--  AVERAGE SHIPPING TIME (days) by Ship Mode
SELECT
    Ship_Mode,
    ROUND(AVG(DATEDIFF(Ship_Date, Order_Date)), 1) AS avg_ship_days,
    MIN(DATEDIFF(Ship_Date, Order_Date))           AS min_days,
    MAX(DATEDIFF(Ship_Date, Order_Date))           AS max_days
FROM superstore
GROUP BY Ship_Mode
ORDER BY avg_ship_days;

--  PEAK SALES MONTHS (overall best months)
SELECT
    DATE_FORMAT(Order_Date, '%B') AS month_name,
    MONTH(Order_Date) AS month_num,
    ROUND(AVG(monthly_sales),2) AS avg_monthly_sales
FROM (
    SELECT
        Order_Date,
        ROUND(SUM(Sales),2) AS monthly_sales
    FROM superstore
    GROUP BY YEAR(Order_Date), MONTH(Order_Date), Order_Date
) sub
GROUP BY month_name, month_num
ORDER BY avg_monthly_sales DESC;


--  Total row count confirmation
SELECT COUNT(*) AS total_rows FROM superstore;
-- Expected: 9994

--  Verify no nulls in key columns
SELECT
    SUM(CASE WHEN Row_ID       IS NULL THEN 1 ELSE 0 END) AS null_row_id,
    SUM(CASE WHEN Order_ID     IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN Order_Date   IS NULL THEN 1 ELSE 0 END) AS null_order_date,
    SUM(CASE WHEN Customer_ID  IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
    SUM(CASE WHEN Sales        IS NULL THEN 1 ELSE 0 END) AS null_sales,
    SUM(CASE WHEN Quantity     IS NULL THEN 1 ELSE 0 END) AS null_quantity,
    SUM(CASE WHEN Profit       IS NULL THEN 1 ELSE 0 END) AS null_profit
FROM superstore;

--  Check for negative Sales (data error)
SELECT COUNT(*) AS negative_sales_count
FROM superstore WHERE Sales < 0;

--  Check for zero or negative Quantity
SELECT COUNT(*) AS bad_quantity_count
FROM superstore WHERE Quantity <= 0;

--  Validate Discount range (should be 0.0 – 1.0)
SELECT
    MIN(Discount) AS min_discount,
    MAX(Discount) AS max_discount,
    SUM(CASE WHEN Discount < 0 OR Discount > 1 THEN 1 ELSE 0 END) AS out_of_range
FROM superstore;

--  Ship Date should never be before Order Date
SELECT COUNT(*) AS invalid_ship_dates
FROM superstore
WHERE Ship_Date < Order_Date;

-- Unique customers, products, and orders
SELECT
    COUNT(DISTINCT Customer_ID)  AS unique_customers,
    COUNT(DISTINCT Product_ID)   AS unique_products,
    COUNT(DISTINCT Order_ID)     AS unique_orders
FROM superstore;

-- Grand totals (cross-check with source)
SELECT
    ROUND(SUM(Sales),2)   AS grand_total_sales,
    ROUND(SUM(Profit),2)  AS grand_total_profit,
    SUM(Quantity)          AS grand_total_qty
FROM superstore;

SELECT Region, ROUND(SUM(Sales),2) AS total_sales, 
ROUND(SUM(Profit),2) AS total_profit
FROM superstore GROUP BY Region ORDER BY total_sales DESC;

SELECT Product_Name, ROUND(SUM(Sales),2) AS total_sales,
ROUND(SUM(Profit),2) AS total_profit
FROM superstore GROUP BY Product_Name 
ORDER BY total_sales DESC LIMIT 10;

SELECT DATE_FORMAT(Order_Date,'%Y-%m') AS month_year,
ROUND(SUM(Sales),2) AS monthly_sales
FROM superstore GROUP BY month_year ORDER BY month_year;


-- ============================================================
-- KEY BUSINESS INSIGHTS (run all queries above first)
-- ============================================================
/*
  1. West region generates the highest total sales.
  2. Technology category leads in sales; Furniture has the lowest margin.
  3. Copiers and Phones are top sub-categories by revenue.
  4. Discounts above 20% typically result in losses — a pricing risk.
  5. Q4 (Oct-Dec) consistently peaks in monthly sales.
  6. Standard Class is the most-used ship mode; Same Day is fastest.
  7. Consumer segment drives the most orders.
  8. Some customers have net negative profit — candidates for review.
*/
