#!/usr/bin/env python3
"""
AWS数据湖项目 - 面向对象数据处理模块
版本: 2.0.0
描述: 提供面向对象的数据处理基础类和具体实现
"""

import logging
import json
import os
import sys
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, BotoCoreError

# PySpark imports (with fallback handling)
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import (
        col, sum as spark_sum, avg, count, max as spark_max, min as spark_min,
        year, month, dayofmonth, date_format, when, isnan, isnull,
        regexp_replace, trim, upper, lower, concat_ws, round as spark_round
    )
    from pyspark.sql.types import *
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    DataFrame = None


# =============================================================================
# 配置和数据类
# =============================================================================

@dataclass
class DataLakeConfig:
    """数据湖配置类"""
    project_prefix: str
    environment: str
    aws_region: str
    
    # S3配置
    raw_bucket: Optional[str] = None
    clean_bucket: Optional[str] = None
    analytics_bucket: Optional[str] = None
    
    # Glue配置
    database_name: Optional[str] = None
    
    # Spark配置
    spark_config: Dict[str, Any] = field(default_factory=dict)
    
    # 日志配置
    log_level: str = "INFO"
    
    def __post_init__(self):
        """初始化后处理，设置默认值"""
        if not self.raw_bucket:
            self.raw_bucket = f"{self.project_prefix}-raw-{self.environment}"
        if not self.clean_bucket:
            self.clean_bucket = f"{self.project_prefix}-clean-{self.environment}"
        if not self.analytics_bucket:
            self.analytics_bucket = f"{self.project_prefix}-analytics-{self.environment}"
        if not self.database_name:
            self.database_name = f"{self.project_prefix}-db-{self.environment}"
    
    @classmethod
    def from_environment(cls) -> 'DataLakeConfig':
        """从环境变量创建配置"""
        return cls(
            project_prefix=os.getenv('PROJECT_PREFIX', 'dl-handson'),
            environment=os.getenv('ENVIRONMENT', 'dev'),
            aws_region=os.getenv('AWS_REGION', 'us-east-1'),
            raw_bucket=os.getenv('RAW_BUCKET_NAME'),
            clean_bucket=os.getenv('CLEAN_BUCKET_NAME'),
            analytics_bucket=os.getenv('ANALYTICS_BUCKET_NAME'),
            database_name=os.getenv('GLUE_DATABASE_NAME'),
            log_level=os.getenv('LOG_LEVEL', 'INFO')
        )


@dataclass
class ProcessingResult:
    """数据处理结果类"""
    success: bool
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    metrics: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)


# =============================================================================
# 自定义异常类
# =============================================================================

class DataLakeError(Exception):
    """数据湖操作基础异常"""
    pass


class ConfigurationError(DataLakeError):
    """配置相关异常"""
    pass


class AWSServiceError(DataLakeError):
    """AWS服务相关异常"""
    pass


class DataProcessingError(DataLakeError):
    """数据处理相关异常"""
    pass


# =============================================================================
# 装饰器
# =============================================================================

def handle_aws_errors(func):
    """AWS操作错误处理装饰器"""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            
            if error_code in ['AccessDenied', 'UnauthorizedOperation']:
                raise AWSServiceError(f"权限被拒绝: {error_message}")
            elif error_code in ['InvalidParameter', 'ValidationException']:
                raise ConfigurationError(f"无效配置: {error_message}")
            else:
                raise AWSServiceError(f"AWS服务错误: {error_message}")
        except BotoCoreError as e:
            raise AWSServiceError(f"AWS连接错误: {str(e)}")
        except Exception as e:
            logging.error(f"未预期的错误在 {func.__name__}: {str(e)}")
            raise DataLakeError(f"未预期的错误: {str(e)}")
    
    return wrapper


def log_execution_time(func):
    """记录函数执行时间的装饰器"""
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        logger = logging.getLogger(func.__module__)
        
        try:
            logger.info(f"开始执行: {func.__name__}")
            result = func(*args, **kwargs)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info(f"完成执行: {func.__name__}, 耗时: {duration:.2f}秒")
            return result
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.error(f"执行失败: {func.__name__}, 耗时: {duration:.2f}秒, 错误: {str(e)}")
            raise
    
    return wrapper


# =============================================================================
# 基础数据处理器抽象类
# =============================================================================

class DataProcessor(ABC):
    """数据处理器抽象基类"""
    
    def __init__(self, config: DataLakeConfig):
        self.config = config
        self.logger = self._setup_logging()
        self._validate_config()
    
    def _setup_logging(self) -> logging.Logger:
        """设置结构化日志"""
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(getattr(logging, self.config.log_level.upper()))
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _validate_config(self) -> None:
        """验证配置有效性"""
        required_fields = ['project_prefix', 'environment', 'aws_region']
        
        for field in required_fields:
            if not getattr(self.config, field):
                raise ConfigurationError(f"缺少必需的配置字段: {field}")
    
    @abstractmethod
    def process(self, input_path: str, output_path: str, **kwargs) -> ProcessingResult:
        """处理数据的抽象方法"""
        pass
    
    @abstractmethod
    def validate_input(self, input_path: str) -> bool:
        """验证输入数据的抽象方法"""
        pass
    
    def create_processing_result(
        self, 
        success: bool, 
        message: str, 
        **kwargs
    ) -> ProcessingResult:
        """创建处理结果对象的辅助方法"""
        return ProcessingResult(
            success=success,
            message=message,
            details=kwargs.get('details', {}),
            metrics=kwargs.get('metrics', {}),
            errors=kwargs.get('errors', [])
        )


# =============================================================================
# Glue数据目录管理器
# =============================================================================

class GlueTableManager:
    """AWS Glue表管理器"""
    
    def __init__(self, config: DataLakeConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.glue_client = boto3.client('glue', region_name=config.aws_region)
        
        self._validate_permissions()
    
    @handle_aws_errors
    def _validate_permissions(self) -> None:
        """验证Glue权限"""
        try:
            self.glue_client.get_databases()
        except ClientError as e:
            raise AWSServiceError(f"Glue权限不足: {e}")
    
    @handle_aws_errors
    @log_execution_time
    def create_database(self, database_name: Optional[str] = None) -> bool:
        """创建Glue数据库"""
        db_name = database_name or self.config.database_name
        
        try:
            self.glue_client.create_database(
                DatabaseInput={
                    'Name': db_name,
                    'Description': f'数据湖数据库 - {self.config.project_prefix} - {self.config.environment}'
                }
            )
            self.logger.info(f"成功创建数据库: {db_name}")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'AlreadyExistsException':
                self.logger.info(f"数据库已存在: {db_name}")
                return True
            else:
                raise
    
    @handle_aws_errors
    @log_execution_time
    def create_table(self, table_config: Dict[str, Any]) -> bool:
        """创建Glue表"""
        table_name = table_config['name']
        database_name = table_config.get('database', self.config.database_name)
        
        table_input = {
            'Name': table_name,
            'StorageDescriptor': {
                'Columns': table_config['columns'],
                'Location': table_config['location'],
                'InputFormat': table_config.get('input_format', 'org.apache.hadoop.mapred.TextInputFormat'),
                'OutputFormat': table_config.get('output_format', 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'),
                'SerdeInfo': table_config.get('serde_info', {
                    'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                    'Parameters': {
                        'field.delim': ',',
                        'skip.header.line.count': '1'
                    }
                })
            }
        }
        
        # 添加分区信息（如果存在）
        if 'partition_keys' in table_config:
            table_input['PartitionKeys'] = table_config['partition_keys']
        
        try:
            self.glue_client.create_table(
                DatabaseName=database_name,
                TableInput=table_input
            )
            self.logger.info(f"成功创建表: {database_name}.{table_name}")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'AlreadyExistsException':
                self.logger.info(f"表已存在: {database_name}.{table_name}")
                return True
            else:
                raise
    
    @handle_aws_errors
    def create_tables_from_schema(self, schema_file: str) -> ProcessingResult:
        """从JSON架构文件创建多个表"""
        try:
            with open(schema_file, 'r', encoding='utf-8') as f:
                schemas = json.load(f)
        except FileNotFoundError:
            return ProcessingResult(
                success=False,
                message=f"架构文件不存在: {schema_file}",
                errors=[f"文件不存在: {schema_file}"]
            )
        except json.JSONDecodeError as e:
            return ProcessingResult(
                success=False,
                message=f"JSON格式错误: {str(e)}",
                errors=[f"JSON解析错误: {str(e)}"]
            )
        
        success_count = 0
        error_count = 0
        errors = []
        
        # 首先创建数据库
        if not self.create_database():
            return ProcessingResult(
                success=False,
                message="数据库创建失败",
                errors=["数据库创建失败"]
            )
        
        # 创建表
        for table in schemas.get('tables', []):
            try:
                if self.create_table(table):
                    success_count += 1
                else:
                    error_count += 1
                    errors.append(f"表创建失败: {table.get('name', 'unknown')}")
            except Exception as e:
                error_count += 1
                errors.append(f"表创建异常: {table.get('name', 'unknown')} - {str(e)}")
        
        return ProcessingResult(
            success=error_count == 0,
            message=f"创建了 {success_count} 个表，{error_count} 个失败",
            metrics={
                'success_count': success_count,
                'error_count': error_count,
                'total_count': success_count + error_count
            },
            errors=errors
        )
    
    @handle_aws_errors
    def list_tables(self, database_name: Optional[str] = None) -> List[str]:
        """列出数据库中的所有表"""
        db_name = database_name or self.config.database_name
        
        response = self.glue_client.get_tables(DatabaseName=db_name)
        return [table['Name'] for table in response['TableList']]
    
    @handle_aws_errors
    def delete_table(self, table_name: str, database_name: Optional[str] = None) -> bool:
        """删除表"""
        db_name = database_name or self.config.database_name
        
        try:
            self.glue_client.delete_table(
                DatabaseName=db_name,
                Name=table_name
            )
            self.logger.info(f"成功删除表: {db_name}.{table_name}")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                self.logger.warning(f"表不存在: {db_name}.{table_name}")
                return True
            else:
                raise


# =============================================================================
# Spark分析处理器
# =============================================================================

class SparkAnalyticsProcessor(DataProcessor):
    """Spark分析处理器"""
    
    def __init__(self, config: DataLakeConfig):
        if not PYSPARK_AVAILABLE:
            raise DataProcessingError("PySpark未安装或不可用")
        
        super().__init__(config)
        self.spark_session: Optional[SparkSession] = None
    
    def _create_spark_session(self) -> SparkSession:
        """创建优化的Spark会话"""
        app_name = f"{self.config.project_prefix}-analytics-{self.config.environment}"
        
        # 默认Spark配置
        default_config = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.parquet.compression.codec": "snappy",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.InstanceProfileCredentialsProvider"
        }
        
        # 合并用户配置
        spark_config = {**default_config, **self.config.spark_config}
        
        builder = SparkSession.builder.appName(app_name)
        
        for key, value in spark_config.items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        
        # 设置日志级别
        spark.sparkContext.setLogLevel("WARN")
        
        self.logger.info(f"Spark会话已创建: {app_name}")
        return spark
    
    @property
    def spark(self) -> SparkSession:
        """懒加载Spark会话"""
        if self.spark_session is None:
            self.spark_session = self._create_spark_session()
        return self.spark_session
    
    def validate_input(self, input_path: str) -> bool:
        """验证输入数据"""
        # 检查S3路径是否存在和可访问
        import boto3
        
        if input_path.startswith('s3://') or input_path.startswith('s3a://'):
            s3_client = boto3.client('s3', region_name=self.config.aws_region)
            
            # 解析S3路径
            path_parts = input_path.replace('s3://', '').replace('s3a://', '').split('/', 1)
            bucket = path_parts[0]
            prefix = path_parts[1] if len(path_parts) > 1 else ''
            
            try:
                response = s3_client.list_objects_v2(
                    Bucket=bucket,
                    Prefix=prefix,
                    MaxKeys=1
                )
                
                if 'Contents' not in response:
                    self.logger.warning(f"输入路径为空: {input_path}")
                    return False
                
                self.logger.info(f"输入验证通过: {input_path}")
                return True
                
            except ClientError as e:
                self.logger.error(f"输入验证失败: {input_path} - {str(e)}")
                return False
        
        # 对于本地路径
        return Path(input_path).exists()
    
    @log_execution_time
    def read_data(self, data_path: str, data_format: str = "csv", **options) -> DataFrame:
        """读取数据的通用方法"""
        self.logger.info(f"从 {data_path} 读取 {data_format} 数据")
        
        reader = self.spark.read.format(data_format)
        
        # 设置默认选项
        if data_format.lower() == "csv":
            default_options = {"header": "true", "inferSchema": "true"}
            options = {**default_options, **options}
        
        for key, value in options.items():
            reader = reader.option(key, value)
        
        return reader.load(data_path)
    
    @log_execution_time
    def write_data(
        self, 
        df: DataFrame, 
        output_path: str, 
        data_format: str = "parquet",
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None,
        **options
    ) -> None:
        """写入数据的通用方法"""
        self.logger.info(f"向 {output_path} 写入 {data_format} 数据")
        
        writer = df.write.format(data_format).mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        for key, value in options.items():
            writer = writer.option(key, value)
        
        writer.save(output_path)
    
    @log_execution_time
    def process(self, input_path: str, output_path: str, **kwargs) -> ProcessingResult:
        """执行Spark分析处理"""
        try:
            if not self.validate_input(input_path):
                return self.create_processing_result(
                    success=False,
                    message="输入验证失败",
                    errors=[f"输入路径无效或不可访问: {input_path}"]
                )
            
            # 执行具体的分析逻辑
            result = self._run_analytics(input_path, output_path, **kwargs)
            
            self.logger.info(f"分析处理完成: {output_path}")
            return result
            
        except Exception as e:
            self.logger.error(f"分析处理失败: {str(e)}")
            return self.create_processing_result(
                success=False,
                message="分析处理失败",
                errors=[str(e)]
            )
    
    def _run_analytics(self, input_path: str, output_path: str, **kwargs) -> ProcessingResult:
        """运行具体的分析逻辑 - 子类可以重写此方法"""
        # 这是基础实现，子类应该重写此方法来实现具体的业务逻辑
        df = self.read_data(input_path)
        
        # 基础分析：计算行数
        row_count = df.count()
        
        # 写入结果
        self.write_data(df, output_path)
        
        return self.create_processing_result(
            success=True,
            message="基础分析完成",
            metrics={"row_count": row_count}
        )
    
    def cleanup(self) -> None:
        """清理资源"""
        if self.spark_session:
            self.spark_session.stop()
            self.spark_session = None
            self.logger.info("Spark会话已停止")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()


# =============================================================================
# 工厂类
# =============================================================================

class DataProcessorFactory:
    """数据处理器工厂类"""
    
    _processors = {
        'spark_analytics': SparkAnalyticsProcessor,
        'glue_tables': GlueTableManager,
    }
    
    @classmethod
    def create_processor(cls, processor_type: str, config: DataLakeConfig):
        """创建数据处理器实例"""
        if processor_type not in cls._processors:
            available_types = ', '.join(cls._processors.keys())
            raise ValueError(f"不支持的处理器类型: {processor_type}. 可用类型: {available_types}")
        
        processor_class = cls._processors[processor_type]
        return processor_class(config)
    
    @classmethod
    def register_processor(cls, name: str, processor_class):
        """注册新的处理器类型"""
        cls._processors[name] = processor_class


# =============================================================================
# 实用工具函数
# =============================================================================

def load_config_from_file(config_path: str) -> DataLakeConfig:
    """从配置文件加载配置"""
    config_file = Path(config_path)
    
    if not config_file.exists():
        raise ConfigurationError(f"配置文件不存在: {config_path}")
    
    # 这里可以根据文件扩展名支持不同的配置格式
    if config_file.suffix.lower() == '.json':
        with open(config_file, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        return DataLakeConfig(**config_data)
    else:
        # 默认从环境变量加载
        return DataLakeConfig.from_environment()


def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None) -> None:
    """设置全局日志配置"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            *([] if log_file is None else [logging.FileHandler(log_file)])
        ]
    )


# =============================================================================
# 主函数示例
# =============================================================================

def main():
    """主函数示例"""
    import argparse
    
    parser = argparse.ArgumentParser(description='数据湖数据处理')
    parser.add_argument('--processor', choices=['spark_analytics', 'glue_tables'], 
                       required=True, help='处理器类型')
    parser.add_argument('--input', help='输入路径')
    parser.add_argument('--output', help='输出路径')
    parser.add_argument('--config', help='配置文件路径')
    parser.add_argument('--log-level', default='INFO', help='日志级别')
    
    args = parser.parse_args()
    
    # 设置日志
    setup_logging(args.log_level)
    
    try:
        # 加载配置
        if args.config:
            config = load_config_from_file(args.config)
        else:
            config = DataLakeConfig.from_environment()
        
        # 创建处理器
        processor = DataProcessorFactory.create_processor(args.processor, config)
        
        if args.processor == 'spark_analytics' and args.input and args.output:
            with processor:
                result = processor.process(args.input, args.output)
                
            if result.success:
                print(f"处理成功: {result.message}")
                if result.metrics:
                    print(f"指标: {result.metrics}")
            else:
                print(f"处理失败: {result.message}")
                for error in result.errors:
                    print(f"错误: {error}")
                sys.exit(1)
        
        elif args.processor == 'glue_tables':
            schema_file = args.input or 'scripts/utils/table_schemas.json'
            result = processor.create_tables_from_schema(schema_file)
            
            if result.success:
                print(f"表创建成功: {result.message}")
            else:
                print(f"表创建失败: {result.message}")
                for error in result.errors:
                    print(f"错误: {error}")
                sys.exit(1)
        
        else:
            print("请提供必需的参数")
            parser.print_help()
            sys.exit(1)
            
    except Exception as e:
        logging.error(f"程序执行失败: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()