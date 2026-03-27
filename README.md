# 🏗️ Data Warehouse Project (SQL Server)

## 📌 Overview
This project implements a modern data warehouse using SQL Server, following the Medallion Architecture:

- Bronze Layer → Raw data ingestion  
- Silver Layer → Data cleaning and transformation  
- Gold Layer → Business-ready data model (Star Schema)

The goal is to transform raw CRM and ERP data into structured datasets for analytics, reporting, and business intelligence.

---

## 🧱 Architecture

Bronze Layer → Silver Layer → Gold Layer  
(Raw Data)     (Cleaned Data)   (Analytics Ready)

---

## ⭐ Data Model (Gold Layer)

Fact Table:
- gold.fact_sales

Dimension Tables:
- gold.dim_products
- gold.dim_customers

Schema:

           dim_customers
                |
                |
dim_products --- fact_sales

---

## 📊 Data Catalog

### gold.dim_products

Description:  
Dimension table containing product-related information. Only active products are included.

Business Rule:  
- Only records where prd_end_dt IS NULL

Columns:

| Column Name     | Data Type | Description |
|----------------|----------|-------------|
| product_key    | INT      | Surrogate key generated using ROW_NUMBER() |
| product_id     | INT      | Original product ID |
| product_number | NVARCHAR | Business key |
| product_name   | NVARCHAR | Product name |
| category_id    | INT      | Category ID |
| category       | NVARCHAR | Product category |
| subcategory    | NVARCHAR | Product subcategory |
| maintenance    | NVARCHAR | Maintenance type |
| cost           | DECIMAL  | Product cost |
| product_line   | NVARCHAR | Product line |
| start_date     | DATE     | Product start date |

Source Tables:
- silver.crm_prd_info  
- silver.erp_px_cat_g1v2  

---

### gold.dim_customers

Description:  
Dimension table containing enriched customer data including demographics and location.

Business Rules:
- If cst_gndr != 'n/a', use it  
- Otherwise fallback to ERP gender  
- Default fallback: 'n/a'  

Columns:

| Column Name      | Data Type | Description |
|-----------------|----------|-------------|
| customer_key    | INT      | Surrogate key |
| customer_id     | INT      | Original customer ID |
| customer_number | NVARCHAR | Business key |
| first_name      | NVARCHAR | First name |
| last_name       | NVARCHAR | Last name |
| country         | NVARCHAR | Country |
| marital_status  | NVARCHAR | Marital status |
| gender          | NVARCHAR | Cleaned gender |
| birth_date      | DATE     | Birth date |
| create_date     | DATE     | Record creation date |

Source Tables:
- silver.crm_cust_info  
- silver.erp_cust_az12  
- silver.erp_loc_a101  

---

### gold.fact_sales

Description:  
Fact table capturing transactional sales data.

Columns:

| Column Name   | Data Type | Description |
|--------------|----------|-------------|
| order_number | NVARCHAR | Order ID |
| product_key  | INT      | FK to dim_products |
| customer_key | INT      | FK to dim_customers |
| order_date   | DATE     | Order date |
| shipping_date| DATE     | Shipping date |
| due_date     | DATE     | Due date |
| sales_amount | DECIMAL  | Total sales |
| quantity     | INT      | Quantity sold |
| price        | DECIMAL  | Unit price |

Source Tables:
- silver.crm_sales_details  

Joins:
- product_key → dim_products.product_number  
- customer_key → dim_customers.customer_id  

