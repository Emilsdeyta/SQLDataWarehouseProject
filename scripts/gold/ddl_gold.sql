/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

--===========================================================
-- Create Dimension: gold.dim_customers
--===========================================================
IF OBJECT_ID('gold.dim_customers','V') IS NOT NULL
	DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,  -- Surrogate key
	ci.cst_id						AS customer_id,
	ci.cst_key						AS customer_number,
	ci.cst_firstname				AS first_name,
	ci.cst_lastname					AS last_name,
	la.CNTRY						AS country,
	ci.cst_marital_status			AS marital_status,
	CASE 
		WHEN cst_gndr != 'n/a' THEN cst_gndr -- CRM is the primary source for gender
		ELSE COALESCE(GEN,'n/a')			 -- Fallback to ERP data
	END gender,
	ca.BDATE						AS birthdate,
	cst_create_date					AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.CID
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.CID
GO 

--===========================================================
-- Create Dimension: gold.dim_products
--===========================================================
IF OBJECT_ID('gold.dim_products','V') IS NOT NULL
	DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY prd_start_dt,prd_key) AS product_key, -- Surrogate key
	cpi.prd_id										AS product_id,
	cpi.prd_key										AS product_number,
	cpi.prd_nm										AS product_name,
	cpi.cat_id										AS category_id,
	pcg.CAT											AS category,
	pcg.SUBCAT										AS subcategory,
	pcg.MAINTENANCE									AS maintenance,
	cpi.prd_cost									AS product_cost,
	cpi.prd_line									AS product_line,
	cpi.prd_start_dt								AS start_date
FROM silver.crm_prd_info cpi
LEFT JOIN silver.erp_px_cat_g1v2 pcg
ON cpi.cat_id = pcg.ID
WHERE prd_end_dt IS NULL 	--Filter oll historical data
GO

--===========================================================
-- Create Fact Table: gold.fact_sales
--===========================================================
IF OBJECT_ID('gold.fact_sales','V') IS NOT NULL
	DROP VIEW gold.fact_sales
GO

CREATE VIEW gold.fact_sales AS
SELECT TOP 20
	cs.sls_ord_num	AS order_number,
	ps.product_key	AS product_key,
	dc.customer_key AS customer_key,
	cs.sls_cust_id	AS customer_id,
	cs.sls_order_dt	AS order_date,
	cs.sls_ship_dt	AS shipping_date,
	cs.sls_due_dt	AS due_date,
	cs.sls_sales	AS sales_amount,
	cs.sls_quantity AS quantity,
	cs.sls_price	AS price
FROM silver.crm_sales_details cs
LEFT JOIN gold.dim_products ps
ON cs.sls_prd_key = ps.product_number
LEFT JOIN gold.dim_customers dc
ON cs.sls_cust_id = dc.customer_id





