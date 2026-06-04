CREATE DATABASE IF NOT EXISTS superstore_db;
USE superstore_db;

DROP TABLE IF EXISTS superstore_raw;

CREATE TABLE superstore_raw (
    Row_ID        INT,
    Order_ID      VARCHAR(20),
    Order_Date    VARCHAR(20),
    Ship_Date     VARCHAR(20),
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

SELECT COUNT(*) FROM superstore_raw;


SELECT * FROM superstore_raw LIMIT 5;

-- Customers dimension
CREATE TABLE IF NOT EXISTS customers AS
SELECT DISTINCT
    Customer_ID,
    Customer_Name,
    Segment,
    City,
    State,
    Region
FROM superstore_raw;

--  Products dimension
CREATE TABLE IF NOT EXISTS products AS
SELECT DISTINCT
    Product_ID,
    Product_Name,
    Category,
    Sub_Category
FROM superstore_raw;

-- Orders fact table
CREATE TABLE IF NOT EXISTS orders AS
SELECT
    Row_ID,
    Order_ID,
    Order_Date,
    Ship_Date,
    Ship_Mode,
    Customer_ID,
    Product_ID,
    Sales,
    Quantity,
    Discount,
    Profit
FROM superstore_raw;

-- Orders with above-average sales
SELECT
    o.Order_ID,
    c.Customer_Name,
    c.Region,
    ROUND(o.Sales, 2) AS Sales
FROM orders o
JOIN customers c ON o.Customer_ID = c.Customer_ID
WHERE o.Sales > (
    SELECT AVG(Sales) FROM orders          -- scalar subquery
)
ORDER BY o.Sales DESC
LIMIT 10;


-- Highest single-order sale per customer (correlated subquery)
SELECT 
    c.Customer_ID,
    c.Customer_Name,
    o.Order_ID,
    ROUND(o.Sales, 2) AS max_order_sales
FROM orders o
JOIN customers c ON o.Customer_ID = c.Customer_ID
JOIN (
    SELECT Customer_ID, MAX(Sales) AS max_sales
    FROM orders
    GROUP BY Customer_ID
) AS max_orders 
ON o.Customer_ID = max_orders.Customer_ID 
AND o.Sales = max_orders.max_sales
ORDER BY max_order_sales DESC
LIMIT 10;

-- Total sales + profit per customer (CTE)
WITH customer_sales AS (
    SELECT
        c.Customer_ID,
        c.Customer_Name,
        c.Segment,
        c.Region,
        ROUND(SUM(o.Sales),  2) AS total_sales,
        ROUND(SUM(o.Profit), 2) AS total_profit,
        COUNT(DISTINCT o.Order_ID) AS order_count
    FROM orders o
    JOIN customers c ON o.Customer_ID = c.Customer_ID
    GROUP BY c.Customer_ID, c.Customer_Name, c.Segment, c.Region
)
SELECT *
FROM customer_sales
ORDER BY total_sales DESC
LIMIT 10;

-- ROW_NUMBER / RANK / DENSE_RANK on total sales

SHOW TABLES;
SELECT VERSION();
WITH customer_sales AS (
    SELECT
        c.Customer_ID, c.Customer_Name, c.Segment, c.Region,
        ROUND(SUM(o.Sales), 2) AS total_sales
    FROM orders o
    JOIN customers c ON o.Customer_ID = c.Customer_ID
    GROUP BY c.Customer_ID, c.Customer_Name, c.Segment, c.Region
)
SELECT
    Customer_Name,
    Segment,
    Region,
    total_sales,
    ROW_NUMBER() OVER (ORDER BY total_sales DESC) AS row_num,
    RANK()       OVER (ORDER BY total_sales DESC) AS sales_rank,
    DENSE_RANK() OVER (ORDER BY total_sales DESC) AS `dense_rank`
FROM customer_sales
ORDER BY total_sales DESC
LIMIT 15;


-- Q5b. RANK partitioned by Region (top customer per region)
WITH customer_sales AS (
    SELECT
        c.Customer_ID, c.Customer_Name, c.Segment, c.Region,
        ROUND(SUM(o.Sales), 2) AS total_sales
    FROM orders o
    JOIN customers c ON o.Customer_ID = c.Customer_ID
    GROUP BY c.Customer_ID, c.Customer_Name, c.Segment, c.Region
)
SELECT
    Customer_Name,
    Region,
    total_sales,
    RANK() OVER (PARTITION BY Region ORDER BY total_sales DESC) AS region_rank
FROM customer_sales
ORDER BY Region, region_rank
LIMIT 20;


-- Running total of sales for top-5 customers (year-over-year)
WITH cust_yearly AS (
    SELECT
        c.Customer_Name,
        SUBSTR(o.Order_Date, 7, 4)   AS yr,
        ROUND(SUM(o.Sales), 2)       AS yearly_sales
    FROM orders o
    JOIN customers c ON o.Customer_ID = c.Customer_ID
    GROUP BY c.Customer_Name, yr
),
top5 AS (
    SELECT Customer_Name
    FROM cust_yearly
    GROUP BY Customer_Name
    ORDER BY SUM(yearly_sales) DESC
    LIMIT 5
)
SELECT
    cy.Customer_Name,
    cy.yr,
    cy.yearly_sales,
    ROUND(SUM(cy.yearly_sales) OVER (
        PARTITION BY cy.Customer_Name
        ORDER BY cy.yr
    ), 2) AS running_total
FROM cust_yearly cy
WHERE cy.Customer_Name IN (SELECT Customer_Name FROM top5)
ORDER BY cy.Customer_Name, cy.yr;

WITH customer_sales AS (
    SELECT
        c.Customer_ID,
        c.Customer_Name,
        c.Segment,
        c.Region,
        ROUND(SUM(o.Sales),  2)        AS total_sales,
        ROUND(SUM(o.Profit), 2)        AS total_profit,
        COUNT(DISTINCT o.Order_ID)     AS order_count
    FROM orders o
    JOIN customers c ON o.Customer_ID = c.Customer_ID
    GROUP BY c.Customer_ID, c.Customer_Name, c.Segment, c.Region
),
ranked AS (
    SELECT *,
        RANK() OVER (ORDER BY total_sales  DESC) AS sales_rank,
        RANK() OVER (ORDER BY total_profit DESC) AS profit_rank
    FROM customer_sales
)
SELECT
    Customer_Name,
    Segment,
    Region,
    total_sales,
    total_profit,
    order_count,
    sales_rank,
    profit_rank
FROM ranked
ORDER BY sales_rank;

-- Top 5 customers by revenue
WITH customer_sales AS (
    SELECT c.Customer_ID, c.Customer_Name, c.Segment, c.Region,
           ROUND(SUM(o.Sales),  2) AS total_sales,
           ROUND(SUM(o.Profit), 2) AS total_profit,
           COUNT(DISTINCT o.Order_ID) AS order_count
    FROM orders o
    JOIN customers c ON o.Customer_ID = c.Customer_ID
    GROUP BY c.Customer_ID, c.Customer_Name, c.Segment, c.Region
),
ranked AS (
    SELECT *, RANK() OVER (ORDER BY total_sales DESC) AS sales_rank
    FROM customer_sales
)
SELECT Customer_Name, Segment, Region, total_sales, total_profit, order_count, sales_rank
FROM ranked WHERE sales_rank <= 5;


-- BQ2. Bottom 5 (lowest revenue) customers
WITH customer_sales AS (
    SELECT c.Customer_ID, c.Customer_Name, c.Segment, c.Region,
           ROUND(SUM(o.Sales), 2) AS total_sales,
           COUNT(DISTINCT o.Order_ID) AS order_count
    FROM orders o
    JOIN customers c ON o.Customer_ID = c.Customer_ID
    GROUP BY c.Customer_ID, c.Customer_Name, c.Segment, c.Region
),
ranked AS (
    SELECT *, RANK() OVER (ORDER BY total_sales ASC) AS low_rank
    FROM customer_sales
)
SELECT Customer_Name, Segment, Region, total_sales, order_count, low_rank
FROM ranked WHERE low_rank <= 5;


-- Single-order customers
WITH customer_orders AS (
    SELECT c.Customer_ID, c.Customer_Name, c.Segment, c.Region,
           COUNT(DISTINCT o.Order_ID) AS order_count,
           ROUND(SUM(o.Sales), 2) AS total_sales
    FROM orders o
    JOIN customers c ON o.Customer_ID = c.Customer_ID
    GROUP BY c.Customer_ID, c.Customer_Name, c.Segment, c.Region
)
SELECT Customer_Name, Segment, Region, total_sales
FROM customer_orders
WHERE order_count = 1
ORDER BY total_sales DESC;


-- Above-average sales orders – full detail
SELECT
    o.Order_ID,
    c.Customer_Name,
    c.Region,
    p.Category,
    p.Sub_Category,
    ROUND(o.Sales,   2) AS Sales,
    ROUND(o.Profit,  2) AS Profit,
    o.Quantity,
    o.Discount
FROM orders o
JOIN customers c ON o.Customer_ID = c.Customer_ID
JOIN products  p ON o.Product_ID  = p.Product_ID
WHERE o.Sales > (SELECT AVG(Sales) FROM orders)
ORDER BY o.Sales DESC
LIMIT 20;


-- Region-level performance summary
WITH region_summary AS (
    SELECT
        c.Region,
        COUNT(DISTINCT c.Customer_ID)  AS customers,
        COUNT(DISTINCT o.Order_ID)     AS total_orders,
        ROUND(SUM(o.Sales),   2)       AS total_sales,
        ROUND(AVG(o.Sales),   2)       AS avg_order_sales,
        ROUND(SUM(o.Profit),  2)       AS total_profit,
        ROUND(100.0 * SUM(o.Profit) / SUM(o.Sales), 2) AS profit_margin_pct
    FROM orders o
    JOIN customers c ON o.Customer_ID = c.Customer_ID
    GROUP BY c.Region
)
SELECT *,
    RANK() OVER (ORDER BY total_sales DESC) AS revenue_rank
FROM region_summary
ORDER BY total_sales DESC;


-- Category profitability ranking
WITH cat_data AS (
    SELECT
        p.Category,
        ROUND(SUM(o.Sales),   2)  AS total_sales,
        ROUND(SUM(o.Profit),  2)  AS total_profit,
        COUNT(DISTINCT o.Order_ID) AS order_count,
        ROUND(100.0 * SUM(o.Profit) / SUM(o.Sales), 2) AS profit_margin_pct
    FROM orders o
    JOIN products p ON o.Product_ID = p.Product_ID
    GROUP BY p.Category
)
SELECT *,
    RANK() OVER (ORDER BY total_profit DESC) AS profit_rank
FROM cat_data
ORDER BY total_profit DESC;
