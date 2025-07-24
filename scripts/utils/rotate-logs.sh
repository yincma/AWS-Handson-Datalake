#!/bin/bash
# 日志轮转脚本 - 保留最近7天的日志

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_DIR="$PROJECT_ROOT/logs"

# 设置保留天数
RETENTION_DAYS=7

echo "=========================================="
echo "日志轮转工具"
echo "=========================================="
echo "日志目录: $LOG_DIR"
echo "保留天数: $RETENTION_DAYS"
echo "=========================================="

# 创建日志目录（如果不存在）
mkdir -p "$LOG_DIR"

# 统计日志文件
total_logs=$(find "$LOG_DIR" -type f -name "*.log" 2>/dev/null | wc -l)
echo "当前日志文件总数: $total_logs"

# 查找并删除超过保留期的日志
old_logs=$(find "$LOG_DIR" -type f -name "*.log" -mtime +$RETENTION_DAYS 2>/dev/null | wc -l)
if [[ $old_logs -gt 0 ]]; then
    echo "发现 $old_logs 个过期日志文件"
    find "$LOG_DIR" -type f -name "*.log" -mtime +$RETENTION_DAYS -delete
    echo "已删除过期日志"
else
    echo "没有过期的日志文件"
fi

# 压缩3天前的日志（如果还没压缩）
uncompressed_logs=$(find "$LOG_DIR" -type f -name "*.log" -mtime +3 ! -name "*.gz" 2>/dev/null)
if [[ -n "$uncompressed_logs" ]]; then
    echo "压缩3天前的日志..."
    while IFS= read -r log_file; do
        gzip "$log_file"
        echo "已压缩: $(basename "$log_file")"
    done <<< "$uncompressed_logs"
fi

# 生成日志报告
echo ""
echo "=========================================="
echo "日志轮转完成"
echo "=========================================="

# 显示当前日志统计
current_logs=$(find "$LOG_DIR" -type f \( -name "*.log" -o -name "*.log.gz" \) 2>/dev/null | wc -l)
echo "当前日志文件数: $current_logs"

# 显示磁盘使用情况
if [[ -d "$LOG_DIR" ]]; then
    log_size=$(du -sh "$LOG_DIR" 2>/dev/null | cut -f1)
    echo "日志目录大小: $log_size"
fi

echo ""
echo "提示: 可以将此脚本添加到cron定时任务中自动执行"
echo "例如: 0 2 * * * $SCRIPT_DIR/rotate-logs.sh"