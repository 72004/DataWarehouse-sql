/*
==========================================================
Script: Data Warehouse Database Initialization
Description:
This script is used to create and initialize the 
DataWarehouse database environment.

What this script will do:
1. Check if a database named 'DataWarehouse' already exists.
2. If it exists, the database will be set to SINGLE_USER mode
   to disconnect all active users.
3. The existing DataWarehouse database will then be dropped.
4. A fresh DataWarehouse database will be created.
5. Three schemas will be created inside the database:
      - bronze  : Raw data layer (data ingested from source systems)
      - silver  : Cleaned and transformed data layer
      - gold    : Business-ready data for analytics and reporting

Purpose:
This structure follows the Medallion Architecture approach
commonly used in modern data warehouses and data lakehouses
to organize data processing stages.
==========================================================
*/

USE master ; 

GO

IF EXISTS( select 1 from sys.databases where name = 'DataWarehouse') 
begin 
	alter DATABASE dataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	drop DATABASE dataWarehouse;
END; 

GO

--creating databse
create database DataWarehouse;

GO

use DataWarehouse;

GO

-- creating bronze schema
create schema bronze;

GO

-- creating silver schema
create schema silver;

GO

-- creating gold schema
create schema gold;
