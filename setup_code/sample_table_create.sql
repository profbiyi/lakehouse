-- Connect to PostgreSQL and create a new database if not already done
CREATE DATABASE sample_db;
\c sample_db;

-- Create a new schema
CREATE SCHEMA sample_schema;

-- Create tables within the schema with created_at and updated_at columns
CREATE TABLE sample_schema.customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sample_schema.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES sample_schema.customers(customer_id),
    order_date DATE,
    amount DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sample_schema.products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    description TEXT,
    price DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sample_schema.order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES sample_schema.orders(order_id),
    product_id INT REFERENCES sample_schema.products(product_id),
    quantity INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);





-- Silver Layer: Data Cleansing and Transformation
-- In the silver layer, we typically aim to normalize and enhance the data. 
-- This involves resolving issues like duplicates, nulls, and enforcing consistency.
-- 1. Customers Table: Clean data, remove duplicates, and ensure consistency.
CREATE TABLE silver.silver_customers AS
SELECT DISTINCT
    customer_id,
    LOWER(TRIM(name)) AS cleaned_name,
    LOWER(TRIM(email)) AS cleaned_email,
    created_at,
    updated_at
FROM
    bronze.customers
WHERE
    email IS NOT NULL
    AND LENGTH(email) > 5;

-- 2. Orders Table: Add meaningful information, such as total spent by the customer.
CREATE TABLE silver.silver_orders AS
SELECT
    o.order_id,
    o.customer_id,
    c.name AS customer_name,
    o.order_date,
    o.amount,
    o.created_at,
    o.updated_at
FROM
    bronze.orders o
JOIN
    bronze.customers c ON o.customer_id = c.customer_id
WHERE
    o.amount > 0; -- Exclude invalid orders


-- 3. Products Table: Normalize product information.
CREATE TABLE silver.silver_products AS
SELECT DISTINCT
    product_id,
    LOWER(TRIM(name)) AS cleaned_name,
    description,
    price,
    created_at,
    updated_at
FROM
    bronze.products
WHERE
    price > 0; -- Ensure valid product prices


-- 4. Order Items Table: Join with products and orders for normalized view.
CREATE TABLE silver.silver_order_items AS
SELECT
    oi.order_item_id,
    oi.order_id,
    o.customer_id,
    p.product_id,
    p.name AS product_name,
    oi.quantity,
    (oi.quantity * p.price) AS total_item_cost,
    oi.created_at,
    oi.updated_at
FROM
    bronze.order_items oi
JOIN
    bronze.orders o ON oi.order_id = o.order_id
JOIN
    bronze.products p ON oi.product_id = p.product_id
WHERE
    oi.quantity > 0; -- Ensure valid quantity






-- Gold Layer: Aggregation and Enrichment
-- The gold layer is where you prepare data for reporting by generating aggregate metrics or enriched datasets.


-- 1. Customer Lifetime Value: Calculate total spending per customer.
CREATE TABLE gold.gold_customer_ltv AS
SELECT
    c.customer_id,
    c.cleaned_name AS customer_name,
    SUM(o.amount) AS total_spent,
    COUNT(o.order_id) AS total_orders,
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS most_recent_order_date
FROM
    silver.silver_customers c
JOIN
    silver.silver_orders o ON c.customer_id = o.customer_id
GROUP BY
    c.customer_id, c.cleaned_name;


-- 2. Product Sales Performance: Total quantity sold and revenue by product.
CREATE TABLE gold.gold_product_sales AS
SELECT
    p.product_id,
    p.cleaned_name AS product_name,
    SUM(oi.quantity) AS total_quantity_sold,
    SUM(oi.total_item_cost) AS total_revenue
FROM
    silver.silver_products p
JOIN
    silver.silver_order_items oi ON p.product_id = oi.product_id
GROUP BY
    p.product_id, p.cleaned_name;


-- 3. Daily Sales Summary: Aggregate order data by day.
CREATE TABLE gold.gold_daily_sales_summary AS
SELECT
    o.order_date,
    COUNT(o.order_id) AS total_orders,
    SUM(o.amount) AS total_sales,
    AVG(o.amount) AS avg_order_value
FROM
    silver.silver_orders o
GROUP BY
    o.order_date;








