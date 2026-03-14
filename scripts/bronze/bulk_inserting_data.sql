use DataWarehouse;
/*
- This script loads raw data from CSV files into the Bronze layer tables of the data warehouse.
- It uses the BULK INSERT command to efficiently import large datasets into SQL Server.
- The firstrow = 2 option skips the header row in the CSV files.
- fieldterminator = ',' specifies that the columns in the file are separated by commas.
- tablock improves performance by locking the table during the bulk insert operation.
- Data from both CRM and ERP source systems is loaded into their respective bronze schema tables.
*/



CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	BEGIN TRY
		declare @start_date datetime , @end_date datetime;

		set @start_date = GETDATE();
		PRINT '>> TRUNCATING Table: bronze.crm_cust_info ';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT 'INSERTING DATA INTO : bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		from 'C:\Users\DELL\OneDrive\Desktop\SQL Projects\DATAwAREHOUSEING\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_date = GETDATE();
		PRINT ('Load Duaration' + cast(DATEDIFF(second, @start_date, @end_date) AS nvarchar)+ ' seconds');
		-- select count(*) from bronze.crm_cust_info;
		
		set @start_date = GETDATE();
		PRINT '>> TRUNCATING Table: bronze.crm_prd_info ';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT 'INSERTING DATA INTO : bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		from 'C:\Users\DELL\OneDrive\Desktop\SQL Projects\DATAwAREHOUSEING\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_date = GETDATE();
		PRINT ('Load Duaration' + cast(DATEDIFF(second, @start_date, @end_date) AS nvarchar)+ ' seconds');
		
		-- select * from bronze.crm_prd_info;
		set @start_date = GETDATE();
		PRINT '>> TRUNCATING Table: bronze.crm_sales_details ';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT 'INSERTING DATA INTO : bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		from 'C:\Users\DELL\OneDrive\Desktop\SQL Projects\DATAwAREHOUSEING\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_date = GETDATE();
		PRINT ('Load Duaration' + cast(DATEDIFF(second, @start_date, @end_date) AS nvarchar)+ ' seconds');

		-- select * from bronze.crm_sales_details;
		
		set @start_date = GETDATE();
		PRINT '>> TRUNCATING Table: bronze.erp_cust_az12 ';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT 'INSERTING DATA INTO : bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		from 'C:\Users\DELL\OneDrive\Desktop\SQL Projects\DATAwAREHOUSEING\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_date = GETDATE();
		PRINT ('Load Duaration' + cast(DATEDIFF(second, @start_date, @end_date) AS nvarchar)+ ' seconds');

		-- select count(*) from bronze.erp_cust_az12;
		set @start_date = GETDATE();
		PRINT '>> TRUNCATING Table: bronze.erp_loc_a101 ';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT 'INSERTING DATA INTO : bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		from 'C:\Users\DELL\OneDrive\Desktop\SQL Projects\DATAwAREHOUSEING\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_date = GETDATE();
		PRINT ('Load Duaration' + cast(DATEDIFF(second, @start_date, @end_date) AS nvarchar)+ ' seconds');

		-- select count(*) from bronze.erp_loc_a101;
		set @start_date = GETDATE();
		PRINT '>> TRUNCATING Table: bronze.erp_px_cat_g1v2 ';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT 'INSERTING DATA INTO : bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		from 'C:\Users\DELL\OneDrive\Desktop\SQL Projects\DATAwAREHOUSEING\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_date = GETDATE();
		PRINT ('Load Duaration' + cast(DATEDIFF(second, @start_date, @end_date) AS nvarchar)+ ' seconds');
	END TRY
	BEGIN CATCH
		PRINT ('============================================')
		PRINT('ERROR OCCURED DURING LOADING THE DATA INTO THE TABLES');
		PRINT('ERROR MESSAGE' + ERROR_MESSAGE());
		PRINT('ERROR MESSAGE' + CAST (ERROR_NUMBER() AS nvarchar));
		PRINT('ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR));
	END CATCH
END

