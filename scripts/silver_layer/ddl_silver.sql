
use DataWarehouse;

/*
===============================================================================
Script: Silver Layer Table Creation
Database: DataWarehouse

Description:
    - This script creates the required tables in the Silver layer of the data warehouse.
    - The Silver layer represents cleaned, transformed, and standardized data 
      derived from the Bronze (raw) layer.

Key Features:
    1. Creates structured tables for both CRM and ERP datasets.
    2. Drops and recreates tables where necessary to ensure a fresh schema.
    3. Defines appropriate data types for consistency and performance.
    4. Adds a default audit column (dwh_create_date) to track data load timestamps.

Tables Created:
    - silver.crm_cust_info       → Customer master data
    - silver.crm_prd_info        → Product information
    - silver.crm_sales_details   → Sales transaction data
    - silver.erp_cust_az12       → ERP customer data
    - silver.erp_loc_a101        → Location and country mapping
    - silver.erp_px_cat_g1v2     → Product category hierarchy

Purpose:
    - To prepare a clean and reliable schema for downstream analytics,
      reporting, and further transformation into the Gold layer.

===============================================================================
*/





CREATE TABLE silver.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_material_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()

);

IF OBJECT_ID ('silver.crm_prd_info', 'U') is NOT NULL
    DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    cat_id NVARCHAR(50),
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(100),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

GO

IF OBJECT_ID ('silver.crm_sales_details', 'U') is NOT NULL
    DROP TABLE silver.crm_sales_details;
CREATE TABLE  silver.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

CREATE TABLE silver.erp_cust_az12 (
    cid NVARCHAR(50),
    bdate date,
    gen NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

GO

create table silver.erp_loc_a101(
    cid NVARCHAR(50),
    cntry NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

GO

create table silver.erp_px_cat_g1v2(
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
