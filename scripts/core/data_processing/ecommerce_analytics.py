#!/usr/bin/env python3
"""
电商数据分析处理器
专门用于处理电商数据的分析任务，继承自基础SparkAnalyticsProcessor
"""

import os
import sys
from typing import Dict, Any, Optional
from datetime import datetime

# 添加lib目录到路径以便导入
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'lib'))

from data_processing import SparkAnalyticsProcessor, DataLakeConfig, ProcessingResult

try:
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import (
        col, sum as spark_sum, avg, count, max as spark_max, min as spark_min,
        year, month, dayofmonth, date_format, when, isnan, isnull,
        regexp_replace, trim, upper, lower, concat_ws, round as spark_round,
        desc, asc, coalesce, lit
    )
    from pyspark.sql.types import *
except ImportError:
    print("警告: PySpark未安装，某些功能将不可用")


class EcommerceAnalyticsProcessor(SparkAnalyticsProcessor):
    """电商数据分析处理器"""
    
    def __init__(self, config: DataLakeConfig):
        super().__init__(config)
        
        # 电商特定的配置
        self.clean_bucket = config.clean_bucket
        self.analytics_bucket = config.analytics_bucket
    
    def read_ecommerce_data(self) -> Dict[str, DataFrame]:
        """读取电商数据的所有表"""
        self.logger.info(f"从清洁层读取电商数据: {self.clean_bucket}")
        
        data = {}
        tables = ['customers', 'products', 'orders', 'order_items']
        
        for table in tables:
            try:
                path = f"s3a://{self.clean_bucket}/ecommerce/{table}/"
                df = self.read_data(path, "csv")
                
                # 数据清洗和标准化
                df = self._clean_dataframe(df, table)
                data[table] = df
                
                self.logger.info(f"成功读取 {table}: {df.count()} 行")
                
            except Exception as e:
                self.logger.error(f"读取 {table} 失败: {str(e)}")
                raise
        
        return data
    
    def _clean_dataframe(self, df: DataFrame, table_name: str) -> DataFrame:
        """清洗和标准化数据框"""
        self.logger.debug(f"清洗数据框: {table_name}")
        
        # 通用清洗：去除空值和重复项
        df = df.dropDuplicates()
        
        # 表特定的清洗
        if table_name == 'customers':
            df = self._clean_customers(df)
        elif table_name == 'products':
            df = self._clean_products(df)
        elif table_name == 'orders':
            df = self._clean_orders(df)
        elif table_name == 'order_items':
            df = self._clean_order_items(df)
        
        return df
    
    def _clean_customers(self, df: DataFrame) -> DataFrame:
        """清洗客户数据"""
        return df.select(
            col("customer_id").cast(IntegerType()),
            trim(upper(col("customer_name"))).alias("customer_name"),
            trim(lower(col("email"))).alias("email"),
            trim(col("phone")).alias("phone"),
            trim(col("address")).alias("address"),
            trim(col("city")).alias("city"),
            trim(col("state")).alias("state"),
            trim(col("country")).alias("country"),
            col("registration_date").cast(DateType()),
            trim(col("customer_segment")).alias("customer_segment")
        ).filter(col("customer_id").isNotNull())
    
    def _clean_products(self, df: DataFrame) -> DataFrame:
        """清洗产品数据"""
        return df.select(
            col("product_id").cast(IntegerType()),
            trim(col("product_name")).alias("product_name"),
            trim(col("category")).alias("category"),
            trim(col("subcategory")).alias("subcategory"),
            col("price").cast(DecimalType(10, 2)),
            col("cost").cast(DecimalType(10, 2)),
            trim(col("supplier")).alias("supplier"),
            col("launch_date").cast(DateType())
        ).filter(col("product_id").isNotNull() & (col("price") > 0))
    
    def _clean_orders(self, df: DataFrame) -> DataFrame:
        """清洗订单数据"""
        return df.select(
            col("order_id").cast(IntegerType()),
            col("customer_id").cast(IntegerType()),
            col("order_date").cast(DateType()),
            col("ship_date").cast(DateType()),
            trim(col("ship_mode")).alias("ship_mode"),
            trim(col("segment")).alias("segment"),
            trim(col("region")).alias("region"),
            col("total_amount").cast(DecimalType(12, 2)),
            trim(col("order_status")).alias("order_status")
        ).filter(
            col("order_id").isNotNull() & 
            col("customer_id").isNotNull() & 
            (col("total_amount") > 0)
        )
    
    def _clean_order_items(self, df: DataFrame) -> DataFrame:
        """清洗订单项数据"""
        return df.select(
            col("order_id").cast(IntegerType()),
            col("product_id").cast(IntegerType()),
            col("quantity").cast(IntegerType()),
            col("unit_price").cast(DecimalType(10, 2)),
            col("discount").cast(DecimalType(5, 4)),
            (col("quantity") * col("unit_price") * (1 - col("discount"))).alias("line_total")
        ).filter(
            col("order_id").isNotNull() & 
            col("product_id").isNotNull() & 
            (col("quantity") > 0) & 
            (col("unit_price") > 0)
        )
    
    def create_customer_analytics(self, data: Dict[str, DataFrame]) -> DataFrame:
        """创建客户分析数据"""
        self.logger.info("创建客户分析数据")
        
        customers = data['customers']
        orders = data['orders']
        order_items = data['order_items']
        
        # 客户订单统计
        customer_order_stats = orders.groupBy("customer_id").agg(
            count("order_id").alias("total_orders"),
            spark_sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order_value"),
            spark_max("order_date").alias("last_order_date"),
            spark_min("order_date").alias("first_order_date")
        )
        
        # 客户产品偏好
        customer_product_stats = order_items.join(orders, "order_id") \
            .groupBy("customer_id") \
            .agg(
                spark_sum("quantity").alias("total_items_purchased"),
                count("product_id").alias("unique_products_purchased")
            )
        
        # 合并客户分析数据
        customer_analytics = customers.join(customer_order_stats, "customer_id", "left") \
            .join(customer_product_stats, "customer_id", "left") \
            .select(
                col("customer_id"),
                col("customer_name"),
                col("email"),
                col("city"),
                col("state"),
                col("country"),
                col("customer_segment"),
                col("registration_date"),
                coalesce(col("total_orders"), lit(0)).alias("total_orders"),
                coalesce(col("total_revenue"), lit(0)).alias("total_revenue"),
                coalesce(col("avg_order_value"), lit(0)).alias("avg_order_value"),
                coalesce(col("total_items_purchased"), lit(0)).alias("total_items_purchased"),
                coalesce(col("unique_products_purchased"), lit(0)).alias("unique_products_purchased"),
                col("last_order_date"),
                col("first_order_date")
            )
        
        return customer_analytics
    
    def create_product_analytics(self, data: Dict[str, DataFrame]) -> DataFrame:
        """创建产品分析数据"""
        self.logger.info("创建产品分析数据")
        
        products = data['products']
        orders = data['orders']
        order_items = data['order_items']
        
        # 产品销售统计
        product_sales = order_items.join(orders, "order_id") \
            .groupBy("product_id") \
            .agg(
                spark_sum("quantity").alias("total_quantity_sold"),
                count("order_id").alias("total_orders"),
                spark_sum("line_total").alias("total_revenue"),
                avg("unit_price").alias("avg_selling_price"),
                countDistinct("customer_id").alias("unique_customers")
            )
        
        # 合并产品分析数据
        product_analytics = products.join(product_sales, "product_id", "left") \
            .select(
                col("product_id"),
                col("product_name"),
                col("category"),
                col("subcategory"),
                col("price"),
                col("cost"),
                (col("price") - col("cost")).alias("margin"),
                col("supplier"),
                col("launch_date"),
                coalesce(col("total_quantity_sold"), lit(0)).alias("total_quantity_sold"),
                coalesce(col("total_orders"), lit(0)).alias("total_orders"),
                coalesce(col("total_revenue"), lit(0)).alias("total_revenue"),
                coalesce(col("avg_selling_price"), lit(0)).alias("avg_selling_price"),
                coalesce(col("unique_customers"), lit(0)).alias("unique_customers")
            )
        
        return product_analytics
    
    def create_monthly_sales_analytics(self, data: Dict[str, DataFrame]) -> DataFrame:
        """创建月度销售分析数据"""
        self.logger.info("创建月度销售分析数据")
        
        orders = data['orders']
        order_items = data['order_items']
        products = data['products']
        
        # 合并订单和订单项数据
        sales_data = orders.join(order_items, "order_id") \
            .join(products, "product_id")
        
        # 按年月分组分析
        monthly_sales = sales_data.select(
            year("order_date").alias("year"),
            month("order_date").alias("month"),
            date_format("order_date", "yyyy-MM").alias("year_month"),
            col("category"),
            col("region"),
            col("quantity"),
            col("line_total")
        ).groupBy("year", "month", "year_month", "category", "region") \
        .agg(
            spark_sum("quantity").alias("total_quantity"),
            spark_sum("line_total").alias("total_revenue"),
            count("*").alias("total_transactions"),
            avg("line_total").alias("avg_transaction_value")
        ).orderBy("year", "month", "category", "region")
        
        return monthly_sales
    
    def create_geographic_analytics(self, data: Dict[str, DataFrame]) -> DataFrame:
        """创建地理分析数据"""
        self.logger.info("创建地理分析数据")
        
        customers = data['customers']
        orders = data['orders']
        
        # 按地区分析
        geographic_analytics = customers.join(orders, "customer_id") \
            .groupBy("country", "state", "city", "region") \
            .agg(
                countDistinct("customer_id").alias("unique_customers"),
                count("order_id").alias("total_orders"),
                spark_sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_order_value")
            ).orderBy(desc("total_revenue"))
        
        return geographic_analytics
    
    def _run_analytics(self, input_path: str, output_path: str, **kwargs) -> ProcessingResult:
        """执行电商数据分析"""
        try:
            start_time = datetime.now()
            
            # 读取数据
            data = self.read_ecommerce_data()
            
            # 创建分析数据
            analytics_results = {}
            
            # 客户分析
            customer_analytics = self.create_customer_analytics(data)
            analytics_results['customer_analytics'] = customer_analytics
            self.write_data(
                customer_analytics,
                f"{output_path}/customer_analytics/",
                partition_by=["customer_segment"]
            )
            
            # 产品分析
            product_analytics = self.create_product_analytics(data)
            analytics_results['product_analytics'] = product_analytics
            self.write_data(
                product_analytics,
                f"{output_path}/product_analytics/",
                partition_by=["category"]
            )
            
            # 月度销售分析
            monthly_sales = self.create_monthly_sales_analytics(data)
            analytics_results['monthly_sales'] = monthly_sales
            self.write_data(
                monthly_sales,
                f"{output_path}/monthly_sales/",
                partition_by=["year", "month"]
            )
            
            # 地理分析
            geographic_analytics = self.create_geographic_analytics(data)
            analytics_results['geographic_analytics'] = geographic_analytics
            self.write_data(
                geographic_analytics,
                f"{output_path}/geographic_analytics/",
                partition_by=["country"]
            )
            
            # 计算处理指标
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            
            metrics = {
                'processing_time_seconds': processing_time,
                'tables_processed': len(data),
                'analytics_created': len(analytics_results)
            }
            
            # 添加每个分析表的行数
            for name, df in analytics_results.items():
                metrics[f'{name}_row_count'] = df.count()
            
            return ProcessingResult(
                success=True,
                message="电商数据分析完成",
                metrics=metrics,
                details={
                    'output_path': output_path,
                    'analytics_types': list(analytics_results.keys())
                }
            )
            
        except Exception as e:
            self.logger.error(f"电商数据分析失败: {str(e)}")
            return ProcessingResult(
                success=False,
                message="电商数据分析失败",
                errors=[str(e)]
            )
    
    def generate_summary_report(self, analytics_path: str) -> Dict[str, Any]:
        """生成分析摘要报告"""
        self.logger.info("生成分析摘要报告")
        
        try:
            report = {}
            
            # 读取客户分析数据
            customer_analytics = self.read_data(f"{analytics_path}/customer_analytics/", "parquet")
            
            report['customer_summary'] = {
                'total_customers': customer_analytics.count(),
                'active_customers': customer_analytics.filter(col("total_orders") > 0).count(),
                'avg_customer_value': customer_analytics.agg(avg("total_revenue")).collect()[0][0] or 0,
                'top_customer_segment': customer_analytics.groupBy("customer_segment") \
                    .count().orderBy(desc("count")).first()['customer_segment'] if customer_analytics.count() > 0 else None
            }
            
            # 读取产品分析数据
            product_analytics = self.read_data(f"{analytics_path}/product_analytics/", "parquet")
            
            top_product = product_analytics.orderBy(desc("total_revenue")).first()
            report['product_summary'] = {
                'total_products': product_analytics.count(),
                'products_sold': product_analytics.filter(col("total_quantity_sold") > 0).count(),
                'top_selling_product': top_product['product_name'] if top_product else None,
                'avg_product_revenue': product_analytics.agg(avg("total_revenue")).collect()[0][0] or 0
            }
            
            # 读取月度销售数据
            monthly_sales = self.read_data(f"{analytics_path}/monthly_sales/", "parquet")
            
            total_revenue = monthly_sales.agg(spark_sum("total_revenue")).collect()[0][0] or 0
            report['sales_summary'] = {
                'total_revenue': float(total_revenue),
                'total_transactions': monthly_sales.agg(spark_sum("total_transactions")).collect()[0][0] or 0,
                'avg_monthly_revenue': monthly_sales.agg(avg("total_revenue")).collect()[0][0] or 0
            }
            
            return report
            
        except Exception as e:
            self.logger.error(f"生成摘要报告失败: {str(e)}")
            return {'error': str(e)}


def main():
    """主函数"""
    import argparse
    import json
    
    parser = argparse.ArgumentParser(description='电商数据分析处理器')
    parser.add_argument('--clean-bucket', required=True, help='清洁数据S3桶名称')
    parser.add_argument('--analytics-bucket', required=True, help='分析数据S3桶名称')
    parser.add_argument('--output-prefix', default='analytics', help='输出路径前缀')
    parser.add_argument('--generate-report', action='store_true', help='生成摘要报告')
    parser.add_argument('--aws-region', default='us-east-1', help='AWS区域')
    parser.add_argument('--log-level', default='INFO', help='日志级别')
    
    args = parser.parse_args()
    
    # 创建配置
    config = DataLakeConfig(
        project_prefix='dl-handson',
        environment='dev',
        aws_region=args.aws_region,
        clean_bucket=args.clean_bucket,
        analytics_bucket=args.analytics_bucket,
        log_level=args.log_level
    )
    
    try:
        # 创建处理器
        with EcommerceAnalyticsProcessor(config) as processor:
            
            input_path = f"s3a://{args.clean_bucket}/ecommerce/"
            output_path = f"s3a://{args.analytics_bucket}/{args.output_prefix}/"
            
            # 执行分析
            result = processor.process(input_path, output_path)
            
            if result.success:
                print(f"✅ 分析成功: {result.message}")
                
                if result.metrics:
                    print(f"📊 处理指标:")
                    for key, value in result.metrics.items():
                        print(f"  {key}: {value}")
                
                # 生成摘要报告
                if args.generate_report:
                    print("📋 生成摘要报告...")
                    report = processor.generate_summary_report(output_path)
                    print(json.dumps(report, indent=2, default=str))
                
            else:
                print(f"❌ 分析失败: {result.message}")
                for error in result.errors:
                    print(f"错误: {error}")
                sys.exit(1)
                
    except Exception as e:
        print(f"❌ 程序执行失败: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()