#!/bin/bash
# Log rotation script - Keep logs for the last 7 days

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_DIR="$PROJECT_ROOT/logs"

# Set retention days
RETENTION_DAYS=7

echo "=========================================="
echo "Log Rotation Tool"
echo "=========================================="
echo "Log directory: $LOG_DIR"
echo "Retention days: $RETENTION_DAYS"
echo "========================================="

# Create log directory (if it doesn't exist)
mkdir -p "$LOG_DIR"

# Count log files
total_logs=$(find "$LOG_DIR" -type f -name "*.log" 2>/dev/null | wc -l)
echo "Current total log files: $total_logs"

# Find and delete logs beyond retention period
old_logs=$(find "$LOG_DIR" -type f -name "*.log" -mtime +$RETENTION_DAYS 2>/dev/null | wc -l)
if [[ $old_logs -gt 0 ]]; then
    echo "Found $old_logs expired log files"
    find "$LOG_DIR" -type f -name "*.log" -mtime +$RETENTION_DAYS -delete
    echo "Expired logs deleted"
else
    echo "No expired log files"
fi

# Compress logs older than 3 days (if not already compressed)
uncompressed_logs=$(find "$LOG_DIR" -type f -name "*.log" -mtime +3 ! -name "*.gz" 2>/dev/null)
if [[ -n "$uncompressed_logs" ]]; then
    echo "Compressing logs older than 3 days..."
    while IFS= read -r log_file; do
        gzip "$log_file"
        echo "Compressed: $(basename "$log_file")"
    done <<< "$uncompressed_logs"
fi

# Generate log report
echo ""
echo "=========================================="
echo "Log rotation completed"
echo "=========================================="

# Show current log statistics
current_logs=$(find "$LOG_DIR" -type f \( -name "*.log" -o -name "*.log.gz" \) 2>/dev/null | wc -l)
echo "Current log file count: $current_logs"

# Show disk usage
if [[ -d "$LOG_DIR" ]]; then
    log_size=$(du -sh "$LOG_DIR" 2>/dev/null | cut -f1)
    echo "Log directory size: $log_size"
fi

echo ""
echo "Tip: You can add this script to cron for automatic execution"
echo "Example: 0 2 * * * $SCRIPT_DIR/rotate-logs.sh"