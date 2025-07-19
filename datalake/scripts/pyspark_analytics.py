#!/usr/bin/env python3
"""
PySpark Data Processing Script for AWS Data Lake Analytics Layer

This script processes clean data from the Clean layer and creates aggregated
analytics datasets in the Analytics layer with proper partitioning.

Author: AWS Data Lake Project
Usage: spark-submit pyspark_analytics.py --input-bucket <clean-bucket> --output-bucket <analytics-bucket>
"""

import sys
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, count, max as spark_max, min as spark_min,
    year, month, dayofmonth, date_format, when, isnan, isnull,
    regexp_replace, trim, upper, lower, concat_ws, round as spark_round
)
from pyspark.sql.types import *

def create_spark_session(app_name="DataLakeAnalytics"):
    """Create Spark session with optimized configuration for data lake processing"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
        .getOrCreate()

def read_clean_data(spark, clean_bucket):
    """Read cleaned data from the Clean layer S3 bucket"""
    print(f"Reading clean data from s3a://{clean_bucket}/")
    
    try:
        # Read customers data
        customers_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(f"s3a://{clean_bucket}/ecommerce/customers/")
        
        # Read orders data
        orders_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(f"s3a://{clean_bucket}/ecommerce/orders/")
        
        # Read order items data
        order_items_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(f"s3a://{clean_bucket}/ecommerce/order_items/")
        
        # Read products data
        products_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(f"s3a://{clean_bucket}/ecommerce/products/")
        
        print(f"✓ Customers: {customers_df.count()} records")
        print(f"✓ Orders: {orders_df.count()} records")
        print(f"✓ Order Items: {order_items_df.count()} records")
        print(f"✓ Products: {products_df.count()} records")
        
        return customers_df, orders_df, order_items_df, products_df
    
    except Exception as e:
        print(f"Error reading clean data: {str(e)}")
        return None, None, None, None

def clean_and_standardize_data(customers_df, orders_df, order_items_df, products_df):
    """Apply additional data cleaning and standardization"""
    print("Applying data cleaning and standardization...")
    
    # Clean customers data
    customers_clean = customers_df \
        .withColumn("email", lower(trim(col("email")))) \
        .withColumn("customer_segment", upper(trim(col("customer_segment")))) \
        .filter(col("customer_id").isNotNull())
    
    # Clean and standardize orders data
    orders_clean = orders_df \
        .withColumn("order_status", lower(trim(col("order_status")))) \
        .withColumn("payment_method", lower(trim(col("payment_method")))) \
        .withColumn("currency", upper(trim(col("currency")))) \
        .filter(col("order_id").isNotNull()) \
        .filter(col("customer_id").isNotNull())
    
    # Add date components for partitioning
    orders_clean = orders_clean \
        .withColumn("order_year", year(col("order_date"))) \
        .withColumn("order_month", month(col("order_date"))) \
        .withColumn("order_day", dayofmonth(col("order_date")))
    
    # Clean order items data
    order_items_clean = order_items_df \
        .withColumn("item_status", lower(trim(col("item_status")))) \
        .filter(col("order_id").isNotNull()) \
        .filter(col("product_id").isNotNull()) \
        .filter(col("quantity") > 0)
    
    # Clean products data
    products_clean = products_df \
        .withColumn("category", trim(col("category"))) \
        .withColumn("subcategory", trim(col("subcategory"))) \
        .withColumn("brand", trim(col("brand"))) \
        .filter(col("product_id").isNotNull())
    
    return customers_clean, orders_clean, order_items_clean, products_clean

def create_customer_analytics(customers_df, orders_df, order_items_df, products_df):
    """Create customer-level analytics aggregations"""
    print("Creating customer analytics...")
    
    # Join orders with order items to get detailed order information
    order_details = orders_df.alias("o") \
        .join(order_items_df.alias("oi"), col("o.order_id") == col("oi.order_id"), "inner") \
        .join(products_df.alias("p"), col("oi.product_id") == col("p.product_id"), "inner")
    
    # Customer order summary
    customer_order_summary = order_details.groupBy("customer_id") \
        .agg(
            count("o.order_id").alias("total_orders"),
            spark_sum("total_amount").alias("total_spent"),
            avg("total_amount").alias("avg_order_value"),
            spark_max("order_date").alias("last_order_date"),
            spark_min("order_date").alias("first_order_date"),
            count(when(col("order_status") == "completed", 1)).alias("completed_orders"),
            count(when(col("order_status") == "cancelled", 1)).alias("cancelled_orders"),
            spark_sum("quantity").alias("total_items_purchased"),
            avg("quantity").alias("avg_items_per_order")
        ) \
        .withColumn("total_spent", spark_round("total_spent", 2)) \
        .withColumn("avg_order_value", spark_round("avg_order_value", 2)) \
        .withColumn("avg_items_per_order", spark_round("avg_items_per_order", 1))
    
    # Join with customer information
    customer_analytics = customers_df.alias("c") \
        .join(customer_order_summary.alias("cos"), 
              col("c.customer_id") == col("cos.customer_id"), "left") \
        .select(
            col("c.customer_id"),
            col("c.first_name"),
            col("c.last_name"),
            col("c.email"),
            col("c.country"),
            col("c.customer_segment"),
            col("c.registration_date"),
            col("cos.total_orders"),
            col("cos.total_spent"),
            col("cos.avg_order_value"),
            col("cos.last_order_date"),
            col("cos.first_order_date"),
            col("cos.completed_orders"),
            col("cos.cancelled_orders"),
            col("cos.total_items_purchased"),
            col("cos.avg_items_per_order")
        ) \
        .fillna(0, ["total_orders", "total_spent", "avg_order_value", 
                   "completed_orders", "cancelled_orders", "total_items_purchased", "avg_items_per_order"])
    
    return customer_analytics

def create_product_analytics(orders_df, order_items_df, products_df):
    """Create product-level analytics aggregations"""
    print("Creating product analytics...")
    
    # Join order items with orders and products
    product_sales = order_items_df.alias("oi") \
        .join(orders_df.alias("o"), col("oi.order_id") == col("o.order_id"), "inner") \
        .join(products_df.alias("p"), col("oi.product_id") == col("p.product_id"), "inner") \
        .filter(col("order_status") != "cancelled")
    
    # Product performance metrics
    product_analytics = product_sales.groupBy(
        "product_id", "product_name", "category", "subcategory", "brand", "price"
    ).agg(
        spark_sum("quantity").alias("total_quantity_sold"),
        spark_sum("total_price").alias("total_revenue"),
        count("oi.order_id").alias("total_orders"),
        avg("quantity").alias("avg_quantity_per_order"),
        spark_sum("discount_applied").alias("total_discounts_given"),
        count("customer_id").alias("unique_customers")
    ) \
    .withColumn("total_revenue", spark_round("total_revenue", 2)) \
    .withColumn("avg_quantity_per_order", spark_round("avg_quantity_per_order", 1)) \
    .withColumn("total_discounts_given", spark_round("total_discounts_given", 2)) \
    .withColumn("revenue_per_unit", spark_round(col("total_revenue") / col("total_quantity_sold"), 2))
    
    return product_analytics

def create_monthly_sales_analytics(orders_df, order_items_df):
    """Create monthly sales analytics with time-based partitioning"""
    print("Creating monthly sales analytics...")
    
    # Join orders with order items for complete sales data
    sales_data = orders_df.alias("o") \
        .join(order_items_df.alias("oi"), col("o.order_id") == col("oi.order_id"), "inner") \
        .filter(col("order_status") != "cancelled")
    
    # Monthly aggregations
    monthly_sales = sales_data.groupBy("order_year", "order_month", "currency") \
        .agg(
            count("o.order_id").alias("total_orders"),
            spark_sum("total_amount").alias("total_revenue"),
            spark_sum("discount_amount").alias("total_discounts"),
            spark_sum("tax_amount").alias("total_taxes"),
            spark_sum("shipping_cost").alias("total_shipping"),
            avg("total_amount").alias("avg_order_value"),
            spark_sum("quantity").alias("total_items_sold"),
            count("customer_id").alias("unique_customers")
        ) \
        .withColumn("total_revenue", spark_round("total_revenue", 2)) \
        .withColumn("total_discounts", spark_round("total_discounts", 2)) \
        .withColumn("total_taxes", spark_round("total_taxes", 2)) \
        .withColumn("total_shipping", spark_round("total_shipping", 2)) \
        .withColumn("avg_order_value", spark_round("avg_order_value", 2)) \
        .withColumn("net_revenue", 
                   spark_round(col("total_revenue") - col("total_discounts"), 2))
    
    return monthly_sales

def write_analytics_data(df, output_bucket, table_name, partition_cols=None):
    """Write analytics data to S3 with proper partitioning"""
    output_path = f"s3a://{output_bucket}/analytics/{table_name}"
    print(f"Writing {table_name} to {output_path}")
    
    try:
        writer = df.coalesce(1).write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .format("parquet")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        writer.save(output_path)
        print(f"✓ Successfully wrote {df.count()} records to {table_name}")
        
    except Exception as e:
        print(f"✗ Error writing {table_name}: {str(e)}")

def main():
    """Main processing function"""
    parser = argparse.ArgumentParser(description="PySpark Data Lake Analytics Processing")
    parser.add_argument("--clean-bucket", required=True, help="Clean data S3 bucket name")
    parser.add_argument("--analytics-bucket", required=True, help="Analytics data S3 bucket name")
    parser.add_argument("--app-name", default="DataLakeAnalytics", help="Spark application name")
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("AWS Data Lake Analytics Processing")
    print("=" * 60)
    print(f"Clean Bucket: {args.clean_bucket}")
    print(f"Analytics Bucket: {args.analytics_bucket}")
    print(f"Application: {args.app_name}")
    print("=" * 60)
    
    # Create Spark session
    spark = create_spark_session(args.app_name)
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Read clean data
        customers_df, orders_df, order_items_df, products_df = read_clean_data(
            spark, args.clean_bucket
        )
        
        if any(df is None for df in [customers_df, orders_df, order_items_df, products_df]):
            print("Failed to read input data. Exiting.")
            sys.exit(1)
        
        # Apply additional cleaning and standardization
        customers_clean, orders_clean, order_items_clean, products_clean = \
            clean_and_standardize_data(customers_df, orders_df, order_items_df, products_df)
        
        # Create analytics datasets
        customer_analytics = create_customer_analytics(
            customers_clean, orders_clean, order_items_clean, products_clean
        )
        
        product_analytics = create_product_analytics(
            orders_clean, order_items_clean, products_clean
        )
        
        monthly_sales = create_monthly_sales_analytics(
            orders_clean, order_items_clean
        )
        
        # Write analytics data to S3
        print("\nWriting analytics data to S3...")
        write_analytics_data(customer_analytics, args.analytics_bucket, "customer_analytics")
        write_analytics_data(product_analytics, args.analytics_bucket, "product_analytics")
        write_analytics_data(monthly_sales, args.analytics_bucket, "monthly_sales", 
                           ["order_year", "order_month"])
        
        print("\n" + "=" * 60)
        print("Analytics processing completed successfully!")
        print("=" * 60)
        
        # Show sample results
        print("\nSample Customer Analytics:")
        customer_analytics.show(5, truncate=False)
        
        print("\nSample Product Analytics:")
        product_analytics.orderBy(col("total_revenue").desc()).show(5, truncate=False)
        
        print("\nSample Monthly Sales:")
        monthly_sales.orderBy("order_year", "order_month").show(10, truncate=False)
        
    except Exception as e:
        print(f"Error in main processing: {str(e)}")
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()