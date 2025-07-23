#!/usr/bin/env python3
"""
分布式追踪和监控系统
版本: 1.0.0
描述: 提供操作链路追踪、性能监控和CloudWatch集成
"""

import os
import sys
import json
import time
import uuid
import logging
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field, asdict
from collections import defaultdict

import boto3
from botocore.exceptions import ClientError, BotoCoreError


# =============================================================================
# 数据类定义
# =============================================================================

@dataclass
class SpanContext:
    """追踪上下文"""
    trace_id: str
    span_id: str
    parent_span_id: Optional[str] = None
    baggage: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Span:
    """追踪span"""
    trace_id: str
    span_id: str
    operation_name: str
    service_name: str
    start_time: float
    end_time: Optional[float] = None
    parent_span_id: Optional[str] = None
    tags: Dict[str, Any] = field(default_factory=dict)
    logs: List[Dict[str, Any]] = field(default_factory=list)
    status: str = "ok"  # ok, error, timeout
    duration_ms: Optional[float] = None
    
    def finish(self, end_time: Optional[float] = None):
        """结束span"""
        self.end_time = end_time or time.time()
        self.duration_ms = (self.end_time - self.start_time) * 1000
    
    def log(self, event: str, **kwargs):
        """添加日志事件"""
        log_entry = {
            'timestamp': time.time(),
            'event': event,
            **kwargs
        }
        self.logs.append(log_entry)
    
    def set_tag(self, key: str, value: Any):
        """设置标签"""
        self.tags[key] = value
    
    def set_error(self, error: Exception):
        """设置错误状态"""
        self.status = "error"
        self.set_tag("error", True)
        self.set_tag("error.object", str(error))
        self.set_tag("error.kind", type(error).__name__)


@dataclass
class Metric:
    """CloudWatch指标"""
    namespace: str
    name: str
    value: float
    unit: str = "Count"
    dimensions: Dict[str, str] = field(default_factory=dict)
    timestamp: Optional[datetime] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc)


@dataclass 
class Event:
    """结构化事件"""
    timestamp: datetime
    level: str
    service: str
    operation: str
    message: str
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if isinstance(self.timestamp, str):
            self.timestamp = datetime.fromisoformat(self.timestamp)


# =============================================================================
# 追踪器核心类
# =============================================================================

class DataLakeTracer:
    """数据湖分布式追踪器"""
    
    def __init__(self, service_name: str, aws_region: str = "us-east-1"):
        self.service_name = service_name
        self.aws_region = aws_region
        
        # 存储
        self._spans: Dict[str, Span] = {}
        self._active_spans: Dict[str, str] = {}  # thread_id -> span_id
        self._metrics_buffer: List[Metric] = []
        self._events_buffer: List[Event] = []
        
        # AWS客户端（懒加载）
        self._cloudwatch = None
        self._logs_client = None
        
        # 配置
        self.max_buffer_size = 100
        self.flush_interval = 60  # 秒
        self.enabled = os.getenv('TRACING_ENABLED', 'true').lower() == 'true'
        
        # 后台刷新线程
        self._flush_timer = None
        self._lock = threading.Lock()
        
        # 设置日志
        self.logger = self._setup_logging()
        
        if self.enabled:
            self._start_flush_timer()
    
    def _setup_logging(self) -> logging.Logger:
        """设置结构化日志"""
        logger = logging.getLogger(f"tracer.{self.service_name}")
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            
            # JSON格式化器
            formatter = logging.Formatter(
                '{"timestamp": "%(asctime)s", "level": "%(levelname)s", '
                '"service": "%(name)s", "message": "%(message)s"}'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        logger.setLevel(logging.INFO)
        return logger
    
    @property
    def cloudwatch(self):
        """懒加载CloudWatch客户端"""
        if self._cloudwatch is None:
            self._cloudwatch = boto3.client('cloudwatch', region_name=self.aws_region)
        return self._cloudwatch
    
    @property
    def logs_client(self):
        """懒加载CloudWatch Logs客户端"""
        if self._logs_client is None:
            self._logs_client = boto3.client('logs', region_name=self.aws_region)
        return self._logs_client
    
    def _start_flush_timer(self):
        """启动定时刷新"""
        if self._flush_timer:
            self._flush_timer.cancel()
        
        def flush_periodically():
            self.flush_buffers()
            self._start_flush_timer()
        
        self._flush_timer = threading.Timer(self.flush_interval, flush_periodically)
        self._flush_timer.daemon = True
        self._flush_timer.start()
    
    def create_span(
        self, 
        operation_name: str, 
        parent_span_id: Optional[str] = None,
        **tags
    ) -> Span:
        """创建新的span"""
        if not self.enabled:
            return self._create_noop_span(operation_name)
        
        thread_id = threading.get_ident()
        
        # 确定父span
        if parent_span_id is None:
            parent_span_id = self._active_spans.get(str(thread_id))
        
        # 生成ID
        if parent_span_id:
            parent_span = self._spans.get(parent_span_id)
            trace_id = parent_span.trace_id if parent_span else str(uuid.uuid4())
        else:
            trace_id = str(uuid.uuid4())
        
        span_id = str(uuid.uuid4())
        
        # 创建span
        span = Span(
            trace_id=trace_id,
            span_id=span_id,
            operation_name=operation_name,
            service_name=self.service_name,
            start_time=time.time(),
            parent_span_id=parent_span_id,
            tags=tags
        )
        
        # 存储span
        with self._lock:
            self._spans[span_id] = span
            self._active_spans[str(thread_id)] = span_id
        
        self.logger.info(f"Started span: {operation_name}", extra={
            'trace_id': trace_id,
            'span_id': span_id,
            'parent_span_id': parent_span_id
        })
        
        return span
    
    def _create_noop_span(self, operation_name: str) -> Span:
        """创建无操作span（当追踪禁用时）"""
        return Span(
            trace_id="noop",
            span_id="noop", 
            operation_name=operation_name,
            service_name=self.service_name,
            start_time=time.time()
        )
    
    def finish_span(self, span: Span):
        """结束span"""
        if not self.enabled or span.span_id == "noop":
            return
        
        span.finish()
        
        thread_id = str(threading.get_ident())
        
        with self._lock:
            # 移除活跃span
            if self._active_spans.get(thread_id) == span.span_id:
                if span.parent_span_id:
                    self._active_spans[thread_id] = span.parent_span_id
                else:
                    self._active_spans.pop(thread_id, None)
        
        self.logger.info(f"Finished span: {span.operation_name}", extra={
            'trace_id': span.trace_id,
            'span_id': span.span_id,
            'duration_ms': span.duration_ms,
            'status': span.status
        })
        
        # 发送到CloudWatch（如果span有错误或耗时较长）
        self._maybe_send_span_metrics(span)
    
    def _maybe_send_span_metrics(self, span: Span):
        """条件性发送span指标"""
        try:
            # 发送耗时指标
            self.record_metric(
                namespace="DataLake/Operations",
                name="OperationDuration", 
                value=span.duration_ms,
                unit="Milliseconds",
                dimensions={
                    "Service": span.service_name,
                    "Operation": span.operation_name,
                    "Status": span.status
                }
            )
            
            # 发送错误指标
            if span.status == "error":
                self.record_metric(
                    namespace="DataLake/Operations",
                    name="OperationErrors",
                    value=1,
                    dimensions={
                        "Service": span.service_name,
                        "Operation": span.operation_name,
                        "ErrorType": span.tags.get("error.kind", "Unknown")
                    }
                )
                
        except Exception as e:
            self.logger.warning(f"Failed to send span metrics: {e}")
    
    @contextmanager
    def span(self, operation_name: str, **tags):
        """Span上下文管理器"""
        span = self.create_span(operation_name, **tags)
        
        try:
            yield span
            
        except Exception as e:
            span.set_error(e)
            span.log("exception", error=str(e), error_type=type(e).__name__)
            raise
            
        finally:
            self.finish_span(span)
    
    def record_metric(
        self, 
        namespace: str,
        name: str, 
        value: float,
        unit: str = "Count",
        dimensions: Optional[Dict[str, str]] = None
    ):
        """记录CloudWatch指标"""
        if not self.enabled:
            return
        
        metric = Metric(
            namespace=namespace,
            name=name,
            value=value,
            unit=unit,
            dimensions=dimensions or {}
        )
        
        with self._lock:
            self._metrics_buffer.append(metric)
            
            # 自动刷新缓冲区
            if len(self._metrics_buffer) >= self.max_buffer_size:
                self._flush_metrics()
    
    def record_event(
        self,
        level: str,
        operation: str,
        message: str,
        **metadata
    ):
        """记录结构化事件"""
        if not self.enabled:
            return
        
        thread_id = str(threading.get_ident())
        active_span_id = self._active_spans.get(thread_id)
        active_span = self._spans.get(active_span_id) if active_span_id else None
        
        event = Event(
            timestamp=datetime.now(timezone.utc),
            level=level,
            service=self.service_name,
            operation=operation,
            message=message,
            trace_id=active_span.trace_id if active_span else None,
            span_id=active_span.span_id if active_span else None,
            metadata=metadata
        )
        
        with self._lock:
            self._events_buffer.append(event)
            
            if len(self._events_buffer) >= self.max_buffer_size:
                self._flush_events()
    
    def _flush_metrics(self):
        """刷新指标缓冲区到CloudWatch"""
        if not self._metrics_buffer:
            return
        
        try:
            # 按命名空间分组指标
            metrics_by_namespace = defaultdict(list)
            
            with self._lock:
                for metric in self._metrics_buffer:
                    metrics_by_namespace[metric.namespace].append(metric)
                self._metrics_buffer.clear()
            
            # 发送每个命名空间的指标
            for namespace, metrics in metrics_by_namespace.items():
                metric_data = []
                
                for metric in metrics:
                    data_point = {
                        'MetricName': metric.name,
                        'Value': metric.value,
                        'Unit': metric.unit,
                        'Timestamp': metric.timestamp
                    }
                    
                    if metric.dimensions:
                        data_point['Dimensions'] = [
                            {'Name': k, 'Value': v} 
                            for k, v in metric.dimensions.items()
                        ]
                    
                    metric_data.append(data_point)
                
                # CloudWatch批量发送（每次最多20个指标）
                for i in range(0, len(metric_data), 20):
                    batch = metric_data[i:i+20]
                    
                    self.cloudwatch.put_metric_data(
                        Namespace=namespace,
                        MetricData=batch
                    )
                
                self.logger.debug(f"Sent {len(metrics)} metrics to {namespace}")
                
        except Exception as e:
            self.logger.error(f"Failed to flush metrics: {e}")
    
    def _flush_events(self):
        """刷新事件缓冲区到CloudWatch Logs"""
        if not self._events_buffer:
            return
        
        try:
            log_group_name = f"/aws/datalake/{self.service_name}"
            log_stream_name = f"events-{datetime.now().strftime('%Y-%m-%d-%H')}"
            
            # 确保日志组和流存在
            self._ensure_log_group_exists(log_group_name)
            self._ensure_log_stream_exists(log_group_name, log_stream_name)
            
            # 准备日志事件
            log_events = []
            
            with self._lock:
                for event in self._events_buffer:
                    log_events.append({
                        'timestamp': int(event.timestamp.timestamp() * 1000),
                        'message': json.dumps(asdict(event), default=str, ensure_ascii=False)
                    })
                self._events_buffer.clear()
            
            # 按时间戳排序
            log_events.sort(key=lambda x: x['timestamp'])
            
            # 发送到CloudWatch Logs
            self.logs_client.put_log_events(
                logGroupName=log_group_name,
                logStreamName=log_stream_name,
                logEvents=log_events
            )
            
            self.logger.debug(f"Sent {len(log_events)} events to CloudWatch Logs")
            
        except Exception as e:
            self.logger.error(f"Failed to flush events: {e}")
    
    def _ensure_log_group_exists(self, log_group_name: str):
        """确保日志组存在"""
        try:
            self.logs_client.create_log_group(logGroupName=log_group_name)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceAlreadyExistsException':
                raise
    
    def _ensure_log_stream_exists(self, log_group_name: str, log_stream_name: str):
        """确保日志流存在"""
        try:
            self.logs_client.create_log_stream(
                logGroupName=log_group_name,
                logStreamName=log_stream_name
            )
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceAlreadyExistsException':
                raise
    
    def flush_buffers(self):
        """立即刷新所有缓冲区"""
        if not self.enabled:
            return
        
        self.logger.debug("Flushing monitoring buffers")
        self._flush_metrics()
        self._flush_events()
    
    def get_trace_summary(self, trace_id: str) -> Dict[str, Any]:
        """获取追踪摘要"""
        trace_spans = [
            span for span in self._spans.values() 
            if span.trace_id == trace_id
        ]
        
        if not trace_spans:
            return {}
        
        # 计算总耗时
        root_spans = [span for span in trace_spans if span.parent_span_id is None]
        total_duration = max(span.duration_ms for span in root_spans if span.duration_ms)
        
        # 统计
        error_count = sum(1 for span in trace_spans if span.status == "error")
        span_count = len(trace_spans)
        
        return {
            "trace_id": trace_id,
            "total_duration_ms": total_duration,
            "span_count": span_count,
            "error_count": error_count,
            "status": "error" if error_count > 0 else "ok",
            "service": self.service_name,
            "spans": [
                {
                    "span_id": span.span_id,
                    "operation_name": span.operation_name,
                    "duration_ms": span.duration_ms,
                    "status": span.status,
                    "tags": span.tags
                }
                for span in sorted(trace_spans, key=lambda s: s.start_time)
            ]
        }
    
    def shutdown(self):
        """关闭追踪器"""
        if self._flush_timer:
            self._flush_timer.cancel()
        
        self.flush_buffers()
        self.logger.info("Tracer shutdown complete")


# =============================================================================
# 全局追踪器实例
# =============================================================================

_global_tracer: Optional[DataLakeTracer] = None

def get_tracer(service_name: str = "datalake") -> DataLakeTracer:
    """获取全局追踪器实例"""
    global _global_tracer
    
    if _global_tracer is None:
        aws_region = os.getenv('AWS_REGION', 'us-east-1')
        _global_tracer = DataLakeTracer(service_name, aws_region)
    
    return _global_tracer

def set_tracer(tracer: DataLakeTracer):
    """设置全局追踪器实例"""
    global _global_tracer
    _global_tracer = tracer


# =============================================================================
# 装饰器和便捷函数
# =============================================================================

def trace(operation_name: Optional[str] = None, **tags):
    """追踪装饰器"""
    def decorator(func):
        nonlocal operation_name
        if operation_name is None:
            operation_name = f"{func.__module__}.{func.__name__}"
        
        def wrapper(*args, **kwargs):
            tracer = get_tracer()
            
            with tracer.span(operation_name, **tags) as span:
                # 添加函数参数到标签
                if args:
                    span.set_tag("args_count", len(args))
                if kwargs:
                    span.set_tag("kwargs_keys", list(kwargs.keys()))
                
                return func(*args, **kwargs)
        
        return wrapper
    return decorator

def record_metric(name: str, value: float, **kwargs):
    """记录指标的便捷函数"""
    tracer = get_tracer()
    tracer.record_metric("DataLake/Custom", name, value, **kwargs)

def record_event(level: str, message: str, **kwargs):
    """记录事件的便捷函数"""
    tracer = get_tracer()
    tracer.record_event(level, "custom", message, **kwargs)


# =============================================================================
# 命令行工具
# =============================================================================

def main():
    """命令行工具"""
    import argparse
    
    parser = argparse.ArgumentParser(description='数据湖追踪和监控工具')
    parser.add_argument('--service', default='datalake', help='服务名称')
    parser.add_argument('--operation', required=True, help='操作名称')
    parser.add_argument('--duration', type=float, help='模拟操作耗时（秒）')
    parser.add_argument('--error', action='store_true', help='模拟错误')
    parser.add_argument('--flush', action='store_true', help='立即刷新缓冲区')
    
    args = parser.parse_args()
    
    # 创建追踪器
    tracer = DataLakeTracer(args.service)
    
    try:
        if args.flush:
            print("刷新缓冲区...")
            tracer.flush_buffers()
            print("刷新完成")
            return
        
        # 模拟操作
        with tracer.span(args.operation) as span:
            span.set_tag("cli_test", True)
            
            if args.duration:
                print(f"模拟操作 {args.operation}，耗时 {args.duration}s...")
                time.sleep(args.duration)
            
            if args.error:
                raise ValueError("模拟错误")
            
            print(f"操作 {args.operation} 完成")
        
        # 记录一些指标和事件
        tracer.record_metric("DataLake/Test", "OperationCount", 1)
        tracer.record_event("info", "test", "CLI测试完成", operation=args.operation)
        
        print("追踪数据已记录")
        
    finally:
        tracer.shutdown()


if __name__ == "__main__":
    main()