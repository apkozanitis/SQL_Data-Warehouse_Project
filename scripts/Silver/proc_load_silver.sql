/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

	---------------------------------------------------------
	PRINT('>> Truncating table silver.crm_cust_info')
	TRUNCATE TABLE silver.crm_cust_info
	PRINT('>> Inserting data into silver.crm_cust_info')
	INSERT INTO silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)

	--Data transformations of the bronze.crm_cust_info table 	
	SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname, --Removing any unnecessary spaces from all the string values
	TRIM(cst_lastname) AS cst_lastname,
	CASE WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
		 WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
		 ELSE 'unknown'
	END cst_marital_status, --Normalize marital status to readable format and handling missing data
	CASE WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
		 WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
		 ELSE 'unknown'
	END cst_gndr, --Normalize gender to readable format and handling missing data
	cst_create_date
	FROM(
		SELECT *
		FROM(
			SELECT
			*,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL 
		)t WHERE flag_last=1 --Removing duplicates : Select the most recent record per customer
	)t1;
	---------------------------------------------------------
	PRINT('>> Truncating table silver.crm_prd_info')
	TRUNCATE TABLE silver.crm_prd_info
	PRINT('>> Inserting data into silver.crm_prd_info')
	INSERT INTO silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
	)
	--Data transformations of the bronze.crm_prd_info table
	SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, --Extract category ID
	SUBSTRING(prd_key,7,LEN(prd_key))AS prd_key,       --Extract product key 
	TRIM(prd_nm) AS prd_nm,
	ISNULL(prd_cost,0) AS prd_cost, 
	CASE WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
		 WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line))='S' THEN 'Other sales'
		 WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
		 ELSE 'unknown'
	END AS prd_line, -- Mapping product line codes to readable format
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
	FROM bronze.crm_prd_info --Calculate end date as one day before the next start date
	---------------------------------------------------------
	PRINT('>> Truncating table silver.crm_sales_details')
	TRUNCATE TABLE silver.crm_sales_details
	PRINT('>> Inserting data into silver.crm_sales_details')
	INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)
	--Data transformations of the bronze.crm_sales_details table
	SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN LEN(sls_order_dt)!=8 OR sls_order_dt=0 THEN NULL 
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt, --Handdling invalid data and datatype casting 
	CASE WHEN LEN(sls_ship_dt)!=8 OR sls_ship_dt=0 THEN NULL 
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt, --Handdling invalid data and datatype casting 
	CASE WHEN LEN(sls_due_dt)!=8 OR sls_due_dt=0 THEN NULL 
		 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt, --Handdling invalid data and datatype casting 
	CASE WHEN sls_sales IS NULL OR sls_sales<0 OR sls_sales=0 OR sls_sales!=sls_quantity*ABS(sls_price) THEN sls_quantity*sls_price
		 ELSE sls_sales
	END AS sls_sales, --Recalculate the sales if original value is missing or incorrect
	sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price=0 THEN sls_sales/sls_quantity
		 WHEN sls_price<0 THEN -sls_price
		 ELSE sls_price --Derive price if original value is missing or incorrect
	END AS sls_price
	FROM bronze.crm_sales_details
	---------------------------------------------------------
	PRINT('>> Truncating table silver.erp_cust_az12')
	TRUNCATE TABLE silver.erp_cust_az12
	PRINT('>> Inserting data into silver.erp_cust_az12')
	INSERT INTO silver.erp_cust_az12(
	cid,
	bdate,
	gen
	)
	--Data transformations of the  table bronze.erp_cust_az12
	SELECT
	CASE WHEN LEN(cid)>10 THEN SUBSTRING(cid,4,LEN(cid)) --Revome 'NAS' prefix if present
		 ELSE cid 
	END AS cid,
	CASE WHEN bdate<'1925-01-01' OR bdate>GETDATE() THEN NULL --Set outdated and future birthdays to NULL
		 ELSE bdate
	END AS bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ('Female','F')THEN 'Female' 
		 WHEN UPPER(TRIM(gen)) IN ('Male','M')THEN 'Male'
		 ELSE 'unknown'
	END AS gen --Normalize gender values and unknown cases
	FROM bronze.erp_cust_az12 ;
	---------------------------------------------------------
	PRINT('>> Truncating table silver.erp_loc_a101')
	TRUNCATE TABLE silver.erp_loc_a101
	PRINT('>> Inserting data into silver.erp_loc_a101')
	INSERT INTO silver.erp_loc_a101(
	cid,
	cntry
	)
	--Data transformations of the  table bronze.erp_loc_a101
	SELECT
	REPLACE(cid,'-','') cid, --must be matching with the cst_key from the customer information
	CASE WHEN TRIM(cntry) IN ('USA','US') THEN 'United States'
		 WHEN cntry =' ' OR cntry IS NULL THEN 'unknown'
		 WHEN TRIM(cntry) IN ('Germany','DE') THEN 'Germany'
		 ELSE TRIM(cntry) --Data standardization & Consistency
	END cntry
	FROM bronze.erp_loc_a101
	---------------------------------------------------------
	PRINT('>> Truncating table silver.erp_px_cat_g1v2')
	TRUNCATE TABLE silver.erp_px_cat_g1v2
	PRINT('>> Inserting data into silver.erp_px_cat_g1v2')
	INSERT INTO silver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance
	)
	--Data transformations of the  table bronze.erp_px_cat_g1v2
	SELECT 
	id,
	TRIM(cat),
	TRIM(subcat),
	TRIM(maintenance)
	FROM bronze.erp_px_cat_g1v2;
END 
