/*
===========================================================================
Product Report
===========================================================================
Purpose :
	-This report consolidates key product metrics and behaviours

Highlights : 
	1. Gathers essensial fields such as product name,category and cost
	2. Segments products by revenue to identify High-Performers, Mid-Range or Low-Performers
	3. Aggregates product-level metrics:
		-total orders
		-total sales
		-total quantity sold,
		-total customers (unique)
		-lifespan(in months)
	4. Calculates valuable KPIS :
		-recency (months since last order)
		-average order revenue
		-average monthly revenue
===========================================================================
*/
IF OBJECT_ID('gold.report_products', 'V') IS NOT NULL
    DROP VIEW gold.report_products;
GO

CREATE VIEW gold.report_products AS
WITH base_query AS(
/*--------------------------------------------------
 1. Base Query : Retrieves core columns from tables
----------------------------------------------------*/
SELECT 
order_number,
order_date,
sales_amount,
quantity,
p.product_key,
customer_key,
product_name,
category,
subcategory,
cost
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
	ON f.product_key=p.product_key
)

, aggregarions_query AS(
/*---------------------------------------------------------------------
 2. Product aggregations: Summarizes key metrics at the product level
-----------------------------------------------------------------------*/
SELECT 
product_key,
product_name,
category,
subcategory,
COUNT(DISTINCT(order_number)) AS total_orders,
SUM(sales_amount) AS total_sales,
SUM(cost) AS total_cost,
SUM(sales_amount)-SUM(cost) AS total_profit,
SUM(quantity) AS total_quantity,
ROUND(AVG(CAST(sales_amount AS FLOAT)/NULLIF(quantity,0)),1) AS avg_selling_price,
COUNT(DISTINCT(customer_key)) total_customers,
MIN(order_date) AS first_order_date,
MAX(order_date) AS last_order_date,
DATEDIFF(MONTH,MIN(order_date),MAX(order_date)) AS product_lifespan
FROM base_query
GROUP BY
product_key,
product_name,
category,
subcategory
)

/*---------------------------------------------------------------------
 3. Final Query : Combines all product results into one result
-----------------------------------------------------------------------*/
SELECT 
product_key,
product_name,
category,
subcategory,
CASE WHEN total_sales>50000 THEN 'High-Performer'
	 WHEN total_sales BETWEEN 10000 AND 49000 THEN 'Mid-Performer'
	 ELSE 'Low-Performer'
END AS product_segments,
total_orders,
total_sales,
total_profit,
total_quantity,
total_customers,
avg_selling_price,
last_order_date,
product_lifespan,
--Compute recency
DATEDIFF(MONTH,last_order_date,GETDATE()) AS recency,
--Compute AOR (average order revenue)
CASE 
	 WHEN total_orders=0 THEN 0
	 ELSE total_sales/total_orders
END AS avg_order_revenue,

--Compute avg monthly sales
CASE
	 WHEN product_lifespan=0 THEN total_sales
	 ELSE total_sales/product_lifespan
END AS avg_monthly_revenue

FROM aggregarions_query
