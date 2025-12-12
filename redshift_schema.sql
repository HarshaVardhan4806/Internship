# Schema creation

-- Staging schema
CREATE SCHEMA staging;

-- Dim tables
CREATE TABLE dim_product (
    product_id VARCHAR(10) PRIMARY KEY,
    category VARCHAR(50),
    subcategory VARCHAR(50),
    price DECIMAL(10,2)
) DISTSTYLE KEY DISTKEY(product_id);

CREATE TABLE dim_region (
    region_id INT PRIMARY KEY,
    region_name VARCHAR(20)
) DISTSTYLE ALL;

CREATE TABLE dim_warehouse (
    warehouse_id VARCHAR(10) PRIMARY KEY,
    region VARCHAR(20)
) DISTSTYLE KEY DISTKEY(warehouse_id);

-- Fact tables
CREATE TABLE fact_sales (
    order_id VARCHAR(10) PRIMARY KEY,
    product_id VARCHAR(10) REFERENCES dim_product(product_id),
    warehouse_id VARCHAR(10) REFERENCES dim_warehouse(warehouse_id),
    date_key DATE,
    qty INT,
    total_sales DECIMAL(12,2)
) DISTSTYLE EVEN SORTKEY(date_key, product_id);

CREATE TABLE fact_shipment (
    shipment_id VARCHAR(10) PRIMARY KEY,
    order_id VARCHAR(10) REFERENCES fact_sales(order_id),
    shipment_date DATE,
    delay_days INT,
    status VARCHAR(20)
) DISTSTYLE KEY DISTKEY(order_id) SORTKEY(shipment_date);

-- Load via COPY (run after Glue writes Parquet)
COPY staging.sales_staging
FROM 's3://supply-chain-data-lake/processed/sales/year=2024/'
IAM_ROLE 'arn:aws:iam::YOUR-ACCOUNT:role/RedshiftCopyRole'
FORMAT AS PARQUET;

-- MERGE into facts (upsert example for sales)
INSERT INTO fact_sales (SELECT * FROM staging.sales_staging)
ON CONFLICT (order_id) DO UPDATE SET qty = EXCLUDED.qty, total_sales = EXCLUDED.total_sales;

-- Vacuum/Analyze
VACUUM fact_sales;
ANALYZE fact_sales;