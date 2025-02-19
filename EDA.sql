/*
-----------------------------------------------------------------------------------------------------------------
This script will generate multiple small size reports for the sake of Exloratory Data Analysis of the gold layer.
Usage --> Run the queries one by one to get the insights that you need
-----------------------------------------------------------------------------------------------------------------
*/

-------------------------- Database Exploration--------------------------
--Explore all objects in Database
SELECT * FROM INFORMATION_SCHEMA.TABLES

--Explore all columns in database
SELECT * FROM INFORMATION_SCHEMA.COLUMNS

--------------------------Dimensions Exploration--------------------------
--Countries our customers are coming from 
SELECT DISTINCT country FROM gold.dim_customers

--Explore all categories of the product
SELECT DISTINCT category,subcategory FROM gold.dim_products
ORDER BY 1,2

--------------------------Dates Exploration --------------------------
--Range of order dates
SELECT
MIN(order_date) first_order_date,
MAX(order_date)last_order_date,
DATEDIFF(MONTH,MIN(order_date),MAX(order_date)) order_range_months
FROM gold.fact_sales

--Yougest and oldest customer
SELECT
MIN(birthdate) youngest_birthdate,
MAX(birthdate) oldest_birthdate
FROM gold.dim_customers

--------------------------Measures Exploration --------------------------
SELECT 'Total Sales' AS measure_name,SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total quantity',SUM(quantity) FROM gold.fact_sales
UNION ALL
SELECT 'Average Price', AVG(price) FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Orders',COUNT(DISTINCT(order_number)) FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Products', count(product_name) FROM gold.dim_products
UNION ALL
SELECT 'Total Nr. Customers', count(customer_key) FROM gold.dim_customers


-------------------------- Magnitute Analysis --------------------------
--Total Customers by Country
SELECT
country,
COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY country
ORDER BY total_customers desc

--Total Customers by gender
SELECT
gender,
COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY gender
ORDER BY total_customers desc

--Total products by category
SELECT
category,
COUNT(product_key) AS total_products
FROM gold.dim_products
GROUP BY category
ORDER BY total_products DESC

--Avg costs in each category
SELECT
category,
AVG(cost) AS avg_cost
FROM gold.dim_products
GROUP BY category
ORDER BY avg_cost DESC

--Total revenue for each category 
SELECT
p.category,
SUM(f.sales_amount) total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
	ON p.product_key=f.product_key
GROUP BY p.category
ORDER BY total_revenue DESC

-------------------------- Ranking Analysis --------------------------
--Which 5 products generate the highest revenue?
SELECT TOP 5
p.product_name,
SUM(f.sales_amount) total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
	ON f.product_key=p.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC

--5 worst performing products
SELECT 
p.product_name,
SUM(f.sales_amount) total_revenue,
ROW_NUMBER() OVER (ORDER BY SUM(f.sales_amount)) AS rank_products
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
	ON f.product_key=p.product_key

--Top 10 customers with highest revenue
SELECT TOP 10
c.customer_key,
c.first_name,
c.last_name,
SUM(f.sales_amount) total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
	ON f.customer_key=c.customer_key 
GROUP BY c.customer_key,c.first_name,c.last_name
ORDER BY total_revenue desc
