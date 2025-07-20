#!/usr/bin/env python3
"""
简单的EMR PySpark测试脚本
用于验证EMR集群功能
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

def main():
    # 创建Spark session
    spark = SparkSession.builder \
        .appName("EMR-Test") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    print("=== EMR PySpark 测试开始 ===")
    
    try:
        # 读取customers数据
        print("正在读取customers数据...")
        customers_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("s3://dl-handson-raw-dev/ecommerce/customers/")
        
        print(f"Customers记录数: {customers_df.count()}")
        print("Customers数据schema:")
        customers_df.printSchema()
        print("Customers数据样本:")
        customers_df.show(5)
        
        # 读取orders数据
        print("正在读取orders数据...")
        orders_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("s3://dl-handson-raw-dev/ecommerce/orders/")
        
        print(f"Orders记录数: {orders_df.count()}")
        print("Orders数据样本:")
        orders_df.show(5)
        
        # 简单的数据分析
        print("=== 数据分析结果 ===")
        customer_count = customers_df.count()
        order_count = orders_df.count()
        
        print(f"总客户数: {customer_count}")
        print(f"总订单数: {order_count}")
        
        # 将结果保存到clean bucket
        print("保存测试结果到S3...")
        result_df = spark.createDataFrame([
            ("customers", customer_count),
            ("orders", order_count)
        ], ["table_name", "record_count"])
        
        result_df.write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv("s3://dl-handson-clean-dev/test_results/")
        
        print("=== EMR PySpark 测试成功完成! ===")
        
    except Exception as e:
        print(f"错误: {e}")
        return 1
    
    finally:
        spark.stop()
    
    return 0

if __name__ == "__main__":
    exit(main())