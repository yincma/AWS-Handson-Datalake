#!/usr/bin/env python3
"""
AWS Data Lake Project - Object-Oriented Data Processing Module
Version: 2.0.0
Description: Provides object-oriented data processing base classes and implementations
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
    # Set fallback values for type annotations
    SparkSession = None
    DataFrame = None


# =============================================================================
# Configuration and Data Classes
# =============================================================================

@dataclass
class DataLakeConfig:
    """Data Lake Configuration Class"""
    project_prefix: str
    environment: str
    aws_region: str
    
    # S3 Configuration
    raw_bucket: Optional[str] = None
    clean_bucket: Optional[str] = None
    analytics_bucket: Optional[str] = None
    
    # Glue Configuration
    database_name: Optional[str] = None
    
    # Spark Configuration
    spark_config: Dict[str, Any] = field(default_factory=dict)
    
    # Logging Configuration
    log_level: str = "INFO"
    
    def __post_init__(self):
        """Post-initialization processing, set default values"""
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
        """Create configuration from environment variables"""
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
    """Data processing result class"""
    success: bool
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    metrics: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)


# =============================================================================
# Custom Exception Classes
# =============================================================================

class DataLakeError(Exception):
    """Base exception for data lake operations"""
    pass


class ConfigurationError(DataLakeError):
    """Configuration related exceptions"""
    pass


class AWSServiceError(DataLakeError):
    """AWS service related exceptions"""
    pass


class DataProcessingError(DataLakeError):
    """Data processing related exceptions"""
    pass


# =============================================================================
# Decorators
# =============================================================================

def handle_aws_errors(func):
    """AWS operation error handling decorator"""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            
            if error_code in ['AccessDenied', 'UnauthorizedOperation']:
                raise AWSServiceError(f"Permission denied: {error_message}")
            elif error_code in ['InvalidParameter', 'ValidationException']:
                raise ConfigurationError(f"Invalid configuration: {error_message}")
            else:
                raise AWSServiceError(f"AWS service error: {error_message}")
        except BotoCoreError as e:
            raise AWSServiceError(f"AWS connection error: {str(e)}")
        except Exception as e:
            logging.error(f"Unexpected error in {func.__name__}: {str(e)}")
            raise DataLakeError(f"Unexpected error: {str(e)}")
    
    return wrapper


def log_execution_time(func):
    """Decorator to log function execution time"""
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        logger = logging.getLogger(func.__module__)
        
        try:
            logger.info(f"Starting execution: {func.__name__}")
            result = func(*args, **kwargs)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info(f"Completed execution: {func.__name__}, duration: {duration:.2f} seconds")
            return result
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.error(f"Execution failed: {func.__name__}, duration: {duration:.2f} seconds, error: {str(e)}")
            raise
    
    return wrapper


# =============================================================================
# Base Data Processor Abstract Class
# =============================================================================

class DataProcessor(ABC):
    """Abstract base class for data processors"""
    
    def __init__(self, config: DataLakeConfig):
        self.config = config
        self.logger = self._setup_logging()
        self._validate_config()
    
    def _setup_logging(self) -> logging.Logger:
        """Set up structured logging"""
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
        """Validate configuration validity"""
        required_fields = ['project_prefix', 'environment', 'aws_region']
        
        for field in required_fields:
            if not getattr(self.config, field):
                raise ConfigurationError(f"Missing required configuration field: {field}")
    
    @abstractmethod
    def process(self, input_path: str, output_path: str, **kwargs) -> ProcessingResult:
        """Abstract method for processing data"""
        pass
    
    @abstractmethod
    def validate_input(self, input_path: str) -> bool:
        """Abstract method for validating input data"""
        pass
    
    def create_processing_result(
        self, 
        success: bool, 
        message: str, 
        **kwargs
    ) -> ProcessingResult:
        """Helper method to create processing result object"""
        return ProcessingResult(
            success=success,
            message=message,
            details=kwargs.get('details', {}),
            metrics=kwargs.get('metrics', {}),
            errors=kwargs.get('errors', [])
        )


# =============================================================================
# Glue Data Catalog Manager
# =============================================================================

class GlueTableManager:
    """AWS Glue Table Manager"""
    
    def __init__(self, config: DataLakeConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.glue_client = boto3.client('glue', region_name=config.aws_region)
        
        self._validate_permissions()
    
    @handle_aws_errors
    def _validate_permissions(self) -> None:
        """Validate Glue permissions"""
        try:
            self.glue_client.get_databases()
        except ClientError as e:
            raise AWSServiceError(f"Insufficient Glue permissions: {e}")
    
    @handle_aws_errors
    @log_execution_time
    def create_database(self, database_name: Optional[str] = None) -> bool:
        """Create Glue database"""
        db_name = database_name or self.config.database_name
        
        try:
            self.glue_client.create_database(
                DatabaseInput={
                    'Name': db_name,
                    'Description': f'Data Lake Database - {self.config.project_prefix} - {self.config.environment}'
                }
            )
            self.logger.info(f"Successfully created database: {db_name}")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'AlreadyExistsException':
                self.logger.info(f"Database already exists: {db_name}")
                return True
            else:
                raise
    
    @handle_aws_errors
    @log_execution_time
    def create_table(self, table_config: Dict[str, Any]) -> bool:
        """Create Glue table"""
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
        
        # Add partition information (if exists)
        if 'partition_keys' in table_config:
            table_input['PartitionKeys'] = table_config['partition_keys']
        
        try:
            self.glue_client.create_table(
                DatabaseName=database_name,
                TableInput=table_input
            )
            self.logger.info(f"Successfully created table: {database_name}.{table_name}")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'AlreadyExistsException':
                self.logger.info(f"Table already exists: {database_name}.{table_name}")
                return True
            else:
                raise
    
    @handle_aws_errors
    def create_tables_from_schema(self, schema_file: str) -> ProcessingResult:
        """Create multiple tables from JSON schema file"""
        try:
            with open(schema_file, 'r', encoding='utf-8') as f:
                schemas = json.load(f)
        except FileNotFoundError:
            return ProcessingResult(
                success=False,
                message=f"Schema file not found: {schema_file}",
                errors=[f"File not found: {schema_file}"]
            )
        except json.JSONDecodeError as e:
            return ProcessingResult(
                success=False,
                message=f"JSON format error: {str(e)}",
                errors=[f"JSON parsing error: {str(e)}"]
            )
        
        success_count = 0
        error_count = 0
        errors = []
        
        # Create database first
        if not self.create_database():
            return ProcessingResult(
                success=False,
                message="Database creation failed",
                errors=["Database creation failed"]
            )
        
        # Create tables
        for table in schemas.get('tables', []):
            try:
                if self.create_table(table):
                    success_count += 1
                else:
                    error_count += 1
                    errors.append(f"Table creation failed: {table.get('name', 'unknown')}")
            except Exception as e:
                error_count += 1
                errors.append(f"Table creation exception: {table.get('name', 'unknown')} - {str(e)}")
        
        return ProcessingResult(
            success=error_count == 0,
            message=f"Created {success_count} tables, {error_count} failed",
            metrics={
                'success_count': success_count,
                'error_count': error_count,
                'total_count': success_count + error_count
            },
            errors=errors
        )
    
    @handle_aws_errors
    def list_tables(self, database_name: Optional[str] = None) -> List[str]:
        """List all tables in the database"""
        db_name = database_name or self.config.database_name
        
        response = self.glue_client.get_tables(DatabaseName=db_name)
        return [table['Name'] for table in response['TableList']]
    
    @handle_aws_errors
    def delete_table(self, table_name: str, database_name: Optional[str] = None) -> bool:
        """Delete table"""
        db_name = database_name or self.config.database_name
        
        try:
            self.glue_client.delete_table(
                DatabaseName=db_name,
                Name=table_name
            )
            self.logger.info(f"Successfully deleted table: {db_name}.{table_name}")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                self.logger.warning(f"Table does not exist: {db_name}.{table_name}")
                return True
            else:
                raise


# =============================================================================
# Spark Analytics Processor
# =============================================================================

class SparkAnalyticsProcessor(DataProcessor):
    """Spark Analytics Processor"""
    
    def __init__(self, config: DataLakeConfig):
        if not PYSPARK_AVAILABLE:
            raise DataProcessingError("PySpark is not installed or not available")
        
        super().__init__(config)
        self.spark_session: Optional[SparkSession] = None
    
    def _create_spark_session(self) -> SparkSession:
        """Create optimized Spark session"""
        app_name = f"{self.config.project_prefix}-analytics-{self.config.environment}"
        
        # Default Spark configuration
        default_config = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.parquet.compression.codec": "snappy",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.InstanceProfileCredentialsProvider"
        }
        
        # Merge user configuration
        spark_config = {**default_config, **self.config.spark_config}
        
        builder = SparkSession.builder.appName(app_name)
        
        for key, value in spark_config.items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel("WARN")
        
        self.logger.info(f"Spark session created: {app_name}")
        return spark
    
    @property
    def spark(self) -> SparkSession:
        """Lazy load Spark session"""
        if self.spark_session is None:
            self.spark_session = self._create_spark_session()
        return self.spark_session
    
    def validate_input(self, input_path: str) -> bool:
        """Validate input data"""
        # Check if S3 path exists and is accessible
        import boto3
        
        if input_path.startswith('s3://') or input_path.startswith('s3a://'):
            s3_client = boto3.client('s3', region_name=self.config.aws_region)
            
            # Parse S3 path
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
                    self.logger.warning(f"Input path is empty: {input_path}")
                    return False
                
                self.logger.info(f"Input validation passed: {input_path}")
                return True
                
            except ClientError as e:
                self.logger.error(f"Input validation failed: {input_path} - {str(e)}")
                return False
        
        # For local paths
        return Path(input_path).exists()
    
    @log_execution_time
    def read_data(self, data_path: str, data_format: str = "csv", **options) -> DataFrame:
        """Generic method for reading data"""
        self.logger.info(f"Reading {data_format} data from {data_path}")
        
        reader = self.spark.read.format(data_format)
        
        # Set default options
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
        """Generic method for writing data"""
        self.logger.info(f"Writing {data_format} data to {output_path}")
        
        writer = df.write.format(data_format).mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        for key, value in options.items():
            writer = writer.option(key, value)
        
        writer.save(output_path)
    
    @log_execution_time
    def process(self, input_path: str, output_path: str, **kwargs) -> ProcessingResult:
        """Execute Spark analytics processing"""
        try:
            if not self.validate_input(input_path):
                return self.create_processing_result(
                    success=False,
                    message="Input validation failed",
                    errors=[f"Invalid or inaccessible input path: {input_path}"]
                )
            
            # Execute specific analytics logic
            result = self._run_analytics(input_path, output_path, **kwargs)
            
            self.logger.info(f"Analytics processing completed: {output_path}")
            return result
            
        except Exception as e:
            self.logger.error(f"Analytics processing failed: {str(e)}")
            return self.create_processing_result(
                success=False,
                message="Analytics processing failed",
                errors=[str(e)]
            )
    
    def _run_analytics(self, input_path: str, output_path: str, **kwargs) -> ProcessingResult:
        """Run specific analytics logic - subclasses can override this method"""
        # This is the base implementation, subclasses should override this method to implement specific business logic
        df = self.read_data(input_path)
        
        # Basic analysis: count rows
        row_count = df.count()
        
        # Write results
        self.write_data(df, output_path)
        
        return self.create_processing_result(
            success=True,
            message="Basic analysis completed",
            metrics={"row_count": row_count}
        )
    
    def cleanup(self) -> None:
        """Clean up resources"""
        if self.spark_session:
            self.spark_session.stop()
            self.spark_session = None
            self.logger.info("Spark session stopped")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()


# =============================================================================
# Factory Class
# =============================================================================

class DataProcessorFactory:
    """Data processor factory class"""
    
    _processors = {
        'spark_analytics': SparkAnalyticsProcessor,
        'glue_tables': GlueTableManager,
    }
    
    @classmethod
    def create_processor(cls, processor_type: str, config: DataLakeConfig):
        """Create data processor instance"""
        if processor_type not in cls._processors:
            available_types = ', '.join(cls._processors.keys())
            raise ValueError(f"Unsupported processor type: {processor_type}. Available types: {available_types}")
        
        processor_class = cls._processors[processor_type]
        return processor_class(config)
    
    @classmethod
    def register_processor(cls, name: str, processor_class):
        """Register new processor type"""
        cls._processors[name] = processor_class


# =============================================================================
# Utility Functions
# =============================================================================

def load_config_from_file(config_path: str) -> DataLakeConfig:
    """Load configuration from file"""
    config_file = Path(config_path)
    
    if not config_file.exists():
        raise ConfigurationError(f"Configuration file does not exist: {config_path}")
    
    # Support different config formats based on file extension
    if config_file.suffix.lower() == '.json':
        with open(config_file, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        return DataLakeConfig(**config_data)
    else:
        # Default to loading from environment variables
        return DataLakeConfig.from_environment()


def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None) -> None:
    """Set up global logging configuration"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            *([] if log_file is None else [logging.FileHandler(log_file)])
        ]
    )


# =============================================================================
# Main Function Example
# =============================================================================

def main():
    """Main function example"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Data Lake Data Processing')
    parser.add_argument('--processor', choices=['spark_analytics', 'glue_tables'], 
                       required=True, help='Processor type')
    parser.add_argument('--input', help='Input path')
    parser.add_argument('--output', help='Output path')
    parser.add_argument('--config', help='Configuration file path')
    parser.add_argument('--log-level', default='INFO', help='Log level')
    
    args = parser.parse_args()
    
    # Set up logging
    setup_logging(args.log_level)
    
    try:
        # Load configuration
        if args.config:
            config = load_config_from_file(args.config)
        else:
            config = DataLakeConfig.from_environment()
        
        # Create processor
        processor = DataProcessorFactory.create_processor(args.processor, config)
        
        if args.processor == 'spark_analytics' and args.input and args.output:
            with processor:
                result = processor.process(args.input, args.output)
                
            if result.success:
                print(f"Processing successful: {result.message}")
                if result.metrics:
                    print(f"Metrics: {result.metrics}")
            else:
                print(f"Processing failed: {result.message}")
                for error in result.errors:
                    print(f"Error: {error}")
                sys.exit(1)
        
        elif args.processor == 'glue_tables':
            schema_file = args.input or 'scripts/utils/table_schemas.json'
            result = processor.create_tables_from_schema(schema_file)
            
            if result.success:
                print(f"Table creation successful: {result.message}")
            else:
                print(f"Table creation failed: {result.message}")
                for error in result.errors:
                    print(f"Error: {error}")
                sys.exit(1)
        
        else:
            print("Please provide required parameters")
            parser.print_help()
            sys.exit(1)
            
    except Exception as e:
        logging.error(f"Program execution failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()