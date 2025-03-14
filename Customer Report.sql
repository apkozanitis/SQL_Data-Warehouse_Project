/*
===========================================================================
Customer Report
===========================================================================
Purpose :
	-This report consolidates key customer metrics and behaviours

Highlights : 
	1. Gathers essensial fields such as names,ages and transactional details
	2. Segments customers into categories (VIP,Regular,New) and age groups
	3. Aggregates customer-level metrics:
		-total orders
		-total sales
		-total quantity purchased
		-total products
		-lifespan(in months)
	4. Calculates valuable KPIS :
		-recency (months since last order)
		-average order value
		-average monthly spend
===========================================================================
*/

IF OBJECT_ID('gold.report_customers', 'V') IS NOT NULL
    DROP VIEW gold.report_customers;
GO

CREATE VIEW gold.report_customers AS
WITH base_query AS (
/*--------------------------------------------------
 1. Base Query : Retrieves core columns from tables
----------------------------------------------------*/
SELECT
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name,' ',c.last_name) AS customer_name,
DATEDIFF(YEAR,birthdate,GETDATE()) AS age
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
	ON f.customer_key=c.customer_key
WHERE order_date IS NOT NULL )

,customer_aggregation AS (
/*---------------------------------------------------------------------
 2. Customer aggregations: Summarizes key metrics at the customer level
-----------------------------------------------------------------------*/
SELECT
	customer_key,
	customer_number,
	customer_name,
	age,
	COUNT(order_number) AS total_orders,
	SUM(sales_amount) AS total_sales,
	SUM(quantity) total_quantity,
	COUNT(product_key) AS total_products,
	MAX(order_date) AS last_order_date,
	DATEDIFF(MONTH,MIN(order_date),MAX(order_date)) AS lifespan
FROM base_query
GROUP BY 
customer_key,
customer_number,
customer_name,
age
)

SELECT
	customer_key,
	customer_number,
	customer_name,
	age,
	CASE WHEN age<20 THEN 'under 20'
		 WHEN age BETWEEN 20 AND 29 THEN '20-29'
		 WHEN age BETWEEN 30 AND 39 THEN '30-39'
		 WHEN age BETWEEN 40 AND 49 THEN '40-49'
		 ELSE 'above 50'
	END age_groups,
	CASE WHEN lifespan>=12 AND total_sales>5000 THEN 'VIP'
		 WHEN lifespan>=12 AND total_sales<=5000 THEN 'Regular'
		 ELSE 'New'
	END customer_segments,
	DATEDIFF(MONTH,last_order_date,GETDATE()) recency,
	total_orders,
	total_sales,
	total_quantity,
	total_products,
	last_order_date,
	lifespan,
	-- Compute average order value(AVO)
	CASE WHEN total_orders=0 THEN 0
	ELSE 
	total_sales/total_orders 
	END AS avg_order_value,
	--Compute average monthly spend
	CASE WHEN lifespan=0 THEN total_sales
	ELSE 
	total_sales/lifespan
	END AS avg_monthly_spend
FROM customer_aggregation
