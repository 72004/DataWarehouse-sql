/*
==========================================================
Script Purpose: Create Bronze Layer Tables in Data Warehouse
==========================================================

Description:
This script creates the initial tables in the Bronze layer 
of the Data Warehouse. The Bronze layer is responsible for 
storing raw data exactly as it is received from source systems.

The tables are created to store data coming from two main
source systems:

1. CRM System (Customer Relationship Management)
   - crm_cust_info: Stores customer information.
   - crm_prd_info: Stores product information.
   - crm_sales_details: Stores sales transaction details.

2. ERP System (Enterprise Resource Planning)
   - erp_cust_az12: Stores additional customer demographic data.
   - erp_loc_a101: Stores customer location information.
   - erp_px_cat_g1v2: Stores product category and maintenance data.

These tables act as raw ingestion tables where data from CSV
files or other external sources will be loaded before further
processing in the Silver and Gold layers of the Data Warehouse.

Database Used:
DataWarehouse

==========================================================
*/

USE DataWarehouse;

CREATE TABLE bronze.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_material_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);

CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(100),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);

GO

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

CREATE TABLE bronze.erp_cust_az12 (
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50)
);

GO

CREATE TABLE bronze.erp_loc_a101(
    cid NVARCHAR(50),
    cntry NVARCHAR(50)
);

GO

CREATE TABLE bronze.erp_px_cat_g1v2(
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)
);
