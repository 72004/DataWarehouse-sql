# 📊 Data Catalog (Gold Layer)

## 1. gold.dim_products

### Description
Dimension table containing product-related information. Only active (non-historical) products are included.

### Columns

| Column Name     | Data Type | Description |
|----------------|----------|-------------|
| product_key    | INT      | Surrogate key generated using ROW_NUMBER() |
| product_id     | INT      | Original product ID from source |
| product_number | NVARCHAR | Business key for product |
| product_name   | NVARCHAR | Name of the product |
| category_id    | INT      | Category ID |
| category       | NVARCHAR | Product category |
| subcategory    | NVARCHAR | Product subcategory |
| maintenance    | NVARCHAR | Maintenance type/category |
| cost           | DECIMAL  | Cost of the product |
| product_line   | NVARCHAR | Product line classification |
| start_date     | DATE     | Product start date |

### Source Tables
- `silver.crm_prd_info`
- `silver.erp_px_cat_g1v2`

### Business Rule
- Only records where `prd_end_dt IS NULL` are included (active products only)

---

## 2. gold.dim_customers

### Description
Dimension table containing enriched customer data including demographics and location.

### Columns

| Column Name      | Data Type | Description |
|-----------------|----------|-------------|
| customer_key    | INT      | Surrogate key generated using ROW_NUMBER() |
| customer_id     | INT      | Original customer ID |
| customer_number | NVARCHAR | Business key for customer |
| first_name      | NVARCHAR | Customer first name |
| last_name       | NVARCHAR | Customer last name |
| country         | NVARCHAR | Customer country |
| marital_status  | NVARCHAR | Marital status |
| gender          | NVARCHAR | Gender (cleaned using fallback logic) |
| birth_date      | DATE     | Customer birth date |
| create_date     | DATE     | Record creation date |

### Source Tables
- `silver.crm_cust_info`
- `silver.erp_cust_az12`
- `silver.erp_loc_a101`

### Business Rules
- If `cst_gndr != 'n/a'`, use it  
- Otherwise fallback to `erp_cust_az12.gen`  
- Default fallback: `'n/a'`

---

## 3. gold.fact_sales

### Description
Fact table capturing transactional sales data.

### Columns

| Column Name   | Data Type | Description |
|--------------|----------|-------------|
| order_number | NVARCHAR | Unique order identifier |
| product_key  | INT      | Foreign key to dim_products |
| customer_key | INT      | Foreign key to dim_customers |
| order_date   | DATE     | Order date |
| shipping_date| DATE     | Shipping date |
| due_date     | DATE     | Due date |
| sales_amount | DECIMAL  | Total sales amount |
| quantity     | INT      | Quantity sold |
| price        | DECIMAL  | Unit price |

### Source Tables
- `silver.crm_sales_details`

### Joins
- `product_key` → `dim_products.product_number`
- `customer_key` → `dim_customers.customer_id`
