/*
===============================================================================
Script: Master ETL Load - Silver Layer
Database: DataWarehouse

Description:
    - This script performs a full ETL (Extract, Transform, Load) process
      to populate the Silver layer tables from the Bronze (raw) layer.
    - It standardizes, cleans, and transforms raw data into structured,
      analytics-ready formats.

Key Operations:
    1. Truncates existing data in Silver tables to ensure a fresh full load.
    2. Loads and transforms data from Bronze layer tables.
    3. Applies data cleaning techniques:
        - Trimming text fields
        - Handling NULL and invalid values
        - Standardizing categorical values
        - Deduplicating records using window functions
    4. Performs business logic transformations:
        - Recalculates sales and price values
        - Converts numeric date formats into proper DATE types
        - Derives product end dates using LEAD() function
    5. Tracks execution time for each table load.
    6. Implements error handling using TRY...CATCH blocks.

Tables Processed:
    - CRM:
        • silver.crm_cust_info       (Customer data with deduplication)
        • silver.crm_sales_details   (Sales transactions with validation)
        • silver.crm_prd_info        (Product details with transformations)

    - ERP:
        • silver.erp_px_cat_g1v2     (Product category data)
        • silver.erp_loc_a101        (Location and country standardization)
        • silver.erp_cust_az12       (Customer demographic data)

Purpose:
    - To ensure clean, consistent, and reliable data in the Silver layer
      for downstream analytics, reporting, and Gold layer processing.

===============================================================================
*/


USE DataWarehouse;
GO

create or alter procedure silver.load_silver as

BEGIN

    /* ============================================================
       MASTER ETL SCRIPT (SILVER LAYER LOAD)
    ============================================================ */

    BEGIN TRY

        DECLARE @StartTime DATETIME, @EndTime DATETIME;

        PRINT '=============================================';
        PRINT 'STARTING FULL ETL LOAD';
        PRINT '=============================================';

        /* ============================================================
           1. CRM - CUSTOMER INFO
        ============================================================ */
        SET @StartTime = GETDATE();

        PRINT 'Loading: silver.crm_cust_info';
        PRINT 'Truncating Table: silver.crm_cust_info';

        TRUNCATE TABLE silver.crm_cust_info;

        PRINT 'Inserting Data...';

        INSERT INTO silver.crm_cust_info(
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_material_status,
            cst_gndr,
            cst_create_date
        )
        SELECT 
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE 
                WHEN cst_material_status = 'S' THEN 'SINGLE'
                WHEN cst_material_status = 'M' THEN 'MARRIED'
                ELSE 'n/a'
            END,
            CASE 
                WHEN cst_gndr = 'F' THEN 'FEMALE'
                WHEN cst_gndr = 'M' THEN 'MALE'
                ELSE 'n/a'
            END,
            cst_create_date
        FROM (
            SELECT *, 
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) x
        WHERE flag_last = 1;

        SET @EndTime = GETDATE();
        PRINT 'Completed: crm_cust_info | Time (sec): ' + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS VARCHAR);



        /* ============================================================
           2. CRM - SALES DETAILS
        ============================================================ */
        SET @StartTime = GETDATE();

        PRINT 'Loading: silver.crm_sales_details';
        PRINT 'Truncating Table: silver.crm_sales_details';

        TRUNCATE TABLE silver.crm_sales_details;

        PRINT 'Inserting Data...';

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
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE 
                WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END,
            CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE),
            CAST(CAST(sls_due_dt AS VARCHAR) AS DATE),
            CASE 
                WHEN sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END,
            sls_quantity,
            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity,0)
                ELSE sls_price
            END
        FROM bronze.crm_sales_details;

        SET @EndTime = GETDATE();
        PRINT 'Completed: crm_sales_details | Time (sec): ' + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS VARCHAR);



        /* ============================================================
           3. CRM - PRODUCT INFO
        ============================================================ */
        SET @StartTime = GETDATE();

        PRINT 'Loading: silver.crm_prd_info';
        PRINT 'Truncating Table: silver.crm_prd_info';

        TRUNCATE TABLE silver.crm_prd_info;

        PRINT 'Inserting Data...';

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
        SELECT 
            prd_id,
            REPLACE(SUBSTRING(prd_key,1,5),'-','_'),
            SUBSTRING(prd_key,7,LEN(prd_key)),
            prd_nm,
            ISNULL(prd_cost,0),
            CASE 
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'MOUNTAIN'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'ROAD'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'OTHER SALES'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'TOURING'
                ELSE 'n/a'
            END,
            CAST(prd_start_dt AS DATE),
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE)
        FROM bronze.crm_prd_info;

        SET @EndTime = GETDATE();
        PRINT 'Completed: crm_prd_info | Time (sec): ' + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS VARCHAR);



        /* ============================================================
           4. ERP - CATEGORY
        ============================================================ */
        SET @StartTime = GETDATE();

        PRINT 'Loading: silver.erp_px_cat_g1v2';
        PRINT 'Truncating Table: silver.erp_px_cat_g1v2';

        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT 'Inserting Data...';

        INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
        SELECT id,cat,subcat,maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @EndTime = GETDATE();
        PRINT 'Completed: erp_px_cat_g1v2 | Time (sec): ' + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS VARCHAR);



        /* ============================================================
           5. ERP - LOCATION
        ============================================================ */
        SET @StartTime = GETDATE();

        PRINT 'Loading: silver.erp_loc_a101';
        PRINT 'Truncating Table: silver.erp_loc_a101';

        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT 'Inserting Data...';

        INSERT INTO silver.erp_loc_a101(cid,cntry)
        SELECT
            REPLACE(cid,'-',''),
            CASE 
                WHEN TRIM(cntry) IN ('USA','United States','US') THEN 'United States'
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN cntry IS NULL OR cntry = '' THEN 'n/a'
                ELSE TRIM(cntry)
            END
        FROM bronze.erp_loc_a101;

        SET @EndTime = GETDATE();
        PRINT 'Completed: erp_loc_a101 | Time (sec): ' + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS VARCHAR);



        /* ============================================================
           6. ERP - CUSTOMER
        ============================================================ */
        SET @StartTime = GETDATE();

        PRINT 'Loading: silver.erp_cust_az12';
        PRINT 'Truncating Table: silver.erp_cust_az12';

        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT 'Inserting Data...';

        INSERT INTO silver.erp_cust_az12(cid,bdate,gen)
        SELECT 
            CASE 
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
                ELSE cid
            END,
            CASE 
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
                ELSE 'n/a'
            END
        FROM bronze.erp_cust_az12;

        SET @EndTime = GETDATE();
        PRINT 'Completed: erp_cust_az12 | Time (sec): ' + CAST(DATEDIFF(SECOND,@StartTime,@EndTime) AS VARCHAR);



        PRINT '=============================================';
        PRINT 'FULL ETL LOAD COMPLETED SUCCESSFULLY';
        PRINT '=============================================';

    END TRY


    BEGIN CATCH
        PRINT '=============================================';
        PRINT 'ERROR OCCURRED DURING ETL LOAD';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR);
        PRINT '=============================================';
    END CATCH;

END




