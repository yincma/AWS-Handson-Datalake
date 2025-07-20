-- Athena SQL Queries for Data Lake Analytics
-- These queries demonstrate how to analyze data in the data lake
-- 
-- NOTE: This file contains examples for creating analytics tables.
-- For basic queries, use the main database "dl-handson-db" directly.

-- =============================================================================
-- 1. CREATE EXTERNAL TABLES
-- =============================================================================

-- Create database (run this first)
CREATE DATABASE IF NOT EXISTS "dl-handson-analytics"
COMMENT 'Data Lake Analytics Database'
LOCATION 's3://dl-handson-analytics-dev/analytics/';

-- Customer Analytics Table
CREATE EXTERNAL TABLE IF NOT EXISTS "dl-handson-analytics".customer_analytics (
    customer_id bigint,
    first_name string,
    last_name string,
    email string,
    country string,
    customer_segment string,
    registration_date date,
    total_orders bigint,
    total_spent double,
    avg_order_value double,
    last_order_date date,
    first_order_date date,
    completed_orders bigint,
    cancelled_orders bigint,
    total_items_purchased bigint,
    avg_items_per_order double
)
STORED AS PARQUET
LOCATION 's3://your-analytics-bucket/analytics/customer_analytics/'
TBLPROPERTIES ('has_encrypted_data'='false');

-- Product Analytics Table
CREATE EXTERNAL TABLE IF NOT EXISTS "dl-handson-analytics".product_analytics (
    product_id string,
    product_name string,
    category string,
    subcategory string,
    brand string,
    price double,
    total_quantity_sold bigint,
    total_revenue double,
    total_orders bigint,
    avg_quantity_per_order double,
    total_discounts_given double,
    unique_customers bigint,
    revenue_per_unit double
)
STORED AS PARQUET
LOCATION 's3://your-analytics-bucket/analytics/product_analytics/'
TBLPROPERTIES ('has_encrypted_data'='false');

-- Monthly Sales Table (partitioned)
CREATE EXTERNAL TABLE IF NOT EXISTS "dl-handson-analytics".monthly_sales (
    currency string,
    total_orders bigint,
    total_revenue double,
    total_discounts double,
    total_taxes double,
    total_shipping double,
    avg_order_value double,
    total_items_sold bigint,
    unique_customers bigint,
    net_revenue double
)
PARTITIONED BY (
    order_year int,
    order_month int
)
STORED AS PARQUET
LOCATION 's3://your-analytics-bucket/analytics/monthly_sales/'
TBLPROPERTIES ('has_encrypted_data'='false');

-- Add partitions for monthly sales (update these based on your data)
MSCK REPAIR TABLE "dl-handson-analytics".monthly_sales;

-- =============================================================================
-- 2. CUSTOMER ANALYSIS QUERIES
-- =============================================================================

-- Top 10 customers by total spent
SELECT 
    customer_id,
    first_name,
    last_name,
    country,
    customer_segment,
    total_spent,
    total_orders,
    avg_order_value
FROM "dl-handson-analytics".customer_analytics
WHERE total_spent > 0
ORDER BY total_spent DESC
LIMIT 10;

-- Customer segmentation analysis
SELECT 
    customer_segment,
    country,
    COUNT(*) as customer_count,
    AVG(total_spent) as avg_total_spent,
    AVG(total_orders) as avg_total_orders,
    AVG(avg_order_value) as avg_order_value,
    SUM(total_spent) as total_revenue
FROM "dl-handson-analytics".customer_analytics
WHERE total_spent > 0
GROUP BY customer_segment, country
ORDER BY total_revenue DESC;

-- Customer retention analysis
SELECT 
    CASE 
        WHEN total_orders = 1 THEN 'One-time Customer'
        WHEN total_orders BETWEEN 2 AND 5 THEN 'Regular Customer'
        WHEN total_orders > 5 THEN 'Loyal Customer'
    END as customer_type,
    COUNT(*) as customer_count,
    AVG(total_spent) as avg_total_spent,
    SUM(total_spent) as total_revenue
FROM "dl-handson-analytics".customer_analytics
WHERE total_spent > 0
GROUP BY 
    CASE 
        WHEN total_orders = 1 THEN 'One-time Customer'
        WHEN total_orders BETWEEN 2 AND 5 THEN 'Regular Customer'
        WHEN total_orders > 5 THEN 'Loyal Customer'
    END
ORDER BY total_revenue DESC;

-- =============================================================================
-- 3. PRODUCT ANALYSIS QUERIES
-- =============================================================================

-- Top selling products by revenue
SELECT 
    product_name,
    brand,
    category,
    subcategory,
    price,
    total_quantity_sold,
    total_revenue,
    total_orders,
    unique_customers,
    revenue_per_unit
FROM "dl-handson-analytics".product_analytics
ORDER BY total_revenue DESC
LIMIT 20;

-- Category performance analysis
SELECT 
    category,
    COUNT(*) as product_count,
    SUM(total_quantity_sold) as total_quantity_sold,
    SUM(total_revenue) as total_category_revenue,
    AVG(price) as avg_product_price,
    SUM(unique_customers) as total_unique_customers
FROM "dl-handson-analytics".product_analytics
GROUP BY category
ORDER BY total_category_revenue DESC;

-- Brand performance analysis
SELECT 
    brand,
    COUNT(*) as product_count,
    SUM(total_revenue) as total_brand_revenue,
    AVG(price) as avg_product_price,
    SUM(total_quantity_sold) as total_quantity_sold,
    AVG(revenue_per_unit) as avg_revenue_per_unit
FROM "dl-handson-analytics".product_analytics
GROUP BY brand
ORDER BY total_brand_revenue DESC;

-- Product profitability analysis (high revenue, low discount)
SELECT 
    product_name,
    brand,
    category,
    price,
    total_revenue,
    total_discounts_given,
    (total_revenue - total_discounts_given) as net_revenue,
    (total_discounts_given / total_revenue * 100) as discount_percentage,
    total_quantity_sold
FROM "dl-handson-analytics".product_analytics
WHERE total_revenue > 1000
ORDER BY (total_revenue - total_discounts_given) DESC
LIMIT 15;

-- =============================================================================
-- 4. TIME-BASED ANALYSIS QUERIES
-- =============================================================================

-- Monthly sales trend
SELECT 
    order_year,
    order_month,
    currency,
    total_orders,
    total_revenue,
    total_discounts,
    net_revenue,
    avg_order_value,
    unique_customers
FROM "dl-handson-analytics".monthly_sales
ORDER BY order_year, order_month, currency;

-- Year-over-year growth (if you have multiple years)
WITH monthly_totals AS (
    SELECT 
        order_year,
        order_month,
        SUM(total_revenue) as monthly_revenue,
        SUM(total_orders) as monthly_orders
    FROM "dl-handson-analytics".monthly_sales
    GROUP BY order_year, order_month
)
SELECT 
    order_year,
    order_month,
    monthly_revenue,
    monthly_orders,
    LAG(monthly_revenue) OVER (PARTITION BY order_month ORDER BY order_year) as prev_year_revenue,
    CASE 
        WHEN LAG(monthly_revenue) OVER (PARTITION BY order_month ORDER BY order_year) IS NOT NULL
        THEN ((monthly_revenue - LAG(monthly_revenue) OVER (PARTITION BY order_month ORDER BY order_year)) / 
              LAG(monthly_revenue) OVER (PARTITION BY order_month ORDER BY order_year) * 100)
    END as yoy_growth_percentage
FROM monthly_totals
ORDER BY order_year, order_month;

-- Currency performance comparison
SELECT 
    currency,
    SUM(total_revenue) as total_revenue,
    SUM(total_orders) as total_orders,
    AVG(avg_order_value) as avg_order_value,
    SUM(unique_customers) as total_customers
FROM "dl-handson-analytics".monthly_sales
GROUP BY currency
ORDER BY total_revenue DESC;

-- =============================================================================
-- 5. ADVANCED ANALYTICS QUERIES
-- =============================================================================

-- Customer lifetime value analysis
WITH customer_metrics AS (
    SELECT 
        customer_id,
        customer_segment,
        country,
        total_spent,
        total_orders,
        DATE_DIFF('day', first_order_date, last_order_date) as customer_lifespan_days
    FROM "dl-handson-analytics".customer_analytics
    WHERE total_orders > 1 AND first_order_date IS NOT NULL AND last_order_date IS NOT NULL
)
SELECT 
    customer_segment,
    country,
    COUNT(*) as customer_count,
    AVG(total_spent) as avg_lifetime_value,
    AVG(customer_lifespan_days) as avg_lifespan_days,
    AVG(total_spent / NULLIF(customer_lifespan_days, 0) * 365) as estimated_annual_value
FROM customer_metrics
GROUP BY customer_segment, country
ORDER BY avg_lifetime_value DESC;

-- Product cross-sell analysis (requires original order data)
-- This would need to be implemented in the PySpark script for better performance

-- Cohort analysis for customer retention
-- This would require additional processing in PySpark to create cohort tables

-- =============================================================================
-- 6. DATA QUALITY CHECKS
-- =============================================================================

-- Check for data completeness
SELECT 
    'customer_analytics' as table_name,
    COUNT(*) as total_records,
    COUNT(customer_id) as non_null_customer_id,
    COUNT(total_spent) as non_null_total_spent,
    AVG(total_spent) as avg_total_spent
FROM "dl-handson-analytics".customer_analytics

UNION ALL

SELECT 
    'product_analytics' as table_name,
    COUNT(*) as total_records,
    COUNT(product_id) as non_null_product_id,
    COUNT(total_revenue) as non_null_total_revenue,
    AVG(total_revenue) as avg_total_revenue
FROM "dl-handson-analytics".product_analytics;

-- Check for data freshness
SELECT 
    'customer_analytics' as table_name,
    MAX(last_order_date) as latest_order_date,
    MIN(first_order_date) as earliest_order_date
FROM "dl-handson-analytics".customer_analytics

UNION ALL

SELECT 
    'monthly_sales' as table_name,
    CAST(MAX(order_year) AS VARCHAR) || '-' || CAST(MAX(order_month) AS VARCHAR) as latest_period,
    CAST(MIN(order_year) AS VARCHAR) || '-' || CAST(MIN(order_month) AS VARCHAR) as earliest_period
FROM "dl-handson-analytics".monthly_sales;

-- =============================================================================
-- NOTES:
-- 1. Replace 'your-analytics-bucket' with your actual S3 bucket name
-- 2. Update database name to match your project prefix
-- 3. Run MSCK REPAIR TABLE after adding new partitions
-- 4. Consider using CTAS (CREATE TABLE AS SELECT) for performance optimization
-- 5. Enable query result reuse in Athena settings for cost optimization
-- =============================================================================