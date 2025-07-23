#!/bin/bash

# =============================================================================
# CloudTrail ログ記録モジュール
# バージョン: 1.0.0
# 説明: データレイクのCloudTrail監査ログを管理
# =============================================================================

# 共通ツールライブラリをロード
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
source "$SCRIPT_DIR/../../lib/common.sh"

readonly CLOUDTRAIL_LOGGING_MODULE_VERSION="1.0.0"

# 加载配置（如果尚未加载）
if [[ -z "${PROJECT_PREFIX:-}" ]]; then
    load_config "$PROJECT_ROOT/configs/config.env"
fi

# =============================================================================
# モジュール設定
# =============================================================================

CLOUDTRAIL_NAME="${PROJECT_PREFIX}-cloudtrail-${ENVIRONMENT}"
CLOUDTRAIL_S3_PREFIX="logs/cloudtrail"

# =============================================================================
# 必須関数の実装
# =============================================================================

cloudtrail_logging_validate() {
    print_info "CloudTrailログ記録モジュールの設定を検証"
    
    local validation_errors=0
    
    # 必須環境変数をチェック
    local required_vars=("PROJECT_PREFIX" "ENVIRONMENT" "AWS_REGION" "AWS_ACCOUNT_ID")
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var:-}" ]]; then
            print_error "必須環境変数が不足: $var"
            validation_errors=$((validation_errors + 1))
        fi
    done
    
    # CloudTrail権限を検証
    if ! aws cloudtrail describe-trails &>/dev/null; then
        print_error "CloudTrail権限の検証に失敗"
        validation_errors=$((validation_errors + 1))
    fi
    
    # 依存モジュールをチェック（S3ストレージ、IAMロール）
    local dependencies=("s3" "iam")
    for dep in "${dependencies[@]}"; do
        local dep_stack_name="${PROJECT_PREFIX}-stack-${dep}-${ENVIRONMENT}"
        if ! check_stack_exists "$dep_stack_name"; then
            print_error "依存モジュールが未デプロイ: $dep"
            validation_errors=$((validation_errors + 1))
        fi
    done
    
    if [[ $validation_errors -eq 0 ]]; then
        print_success "CloudTrailログ記録モジュールの検証に合格"
        return 0
    else
        print_error "CloudTrailログ記録モジュールの検証に失敗: $validation_errors 個のエラー"
        return 1
    fi
}

cloudtrail_logging_deploy() {
    print_info "CloudTrailログ記録モジュールをデプロイ"
    
    # S3バケット名を取得
    local s3_stack_name="${PROJECT_PREFIX}-stack-s3-${ENVIRONMENT}"
    local raw_bucket
    
    raw_bucket=$(aws cloudformation describe-stacks \
        --stack-name "$s3_stack_name" \
        --query 'Stacks[0].Outputs[?OutputKey==`RawDataBucketName`].OutputValue' \
        --output text 2>/dev/null)
    
    if [[ -z "$raw_bucket" ]]; then
        print_error "S3バケット名を取得できません"
        return 1
    fi
    
    # CloudTrailの設定準備
    print_info "CloudTrailを設定: S3バケット=$raw_bucket"
    
    # CloudTrailが既存かチェック
    if aws cloudtrail describe-trails --trail-name-list "$CLOUDTRAIL_NAME" &>/dev/null; then
        print_info "既存のCloudTrailを更新: $CLOUDTRAIL_NAME"
        
        aws cloudtrail put-event-selectors \
            --trail-name "$CLOUDTRAIL_NAME" \
            --event-selectors ReadWriteType=All,IncludeManagementEvents=true,DataResources='[{Type=AWS::S3::Object,Values=["'$raw_bucket'/*"]},{Type=AWS::Glue::Table,Values=["*"]}]' &>/dev/null
        
        print_success "CloudTrail設定を更新しました"
    else
        print_info "新しいCloudTrailを作成: $CLOUDTRAIL_NAME"
        
        # CloudTrailを作成
        if aws cloudtrail create-trail \
            --name "$CLOUDTRAIL_NAME" \
            --s3-bucket-name "$raw_bucket" \
            --s3-key-prefix "$CLOUDTRAIL_S3_PREFIX" \
            --include-global-service-events \
            --is-multi-region-trail \
            --enable-log-file-validation \
            --tags-list Key=Project,Value="$PROJECT_PREFIX" Key=Environment,Value="$ENVIRONMENT"; then
            
            # ログ記録を開始
            aws cloudtrail start-logging --name "$CLOUDTRAIL_NAME"
            
            # イベントセレクターを設定
            aws cloudtrail put-event-selectors \
                --trail-name "$CLOUDTRAIL_NAME" \
                --event-selectors ReadWriteType=All,IncludeManagementEvents=true,DataResources='[{Type=AWS::S3::Object,Values=["'$raw_bucket'/*"]},{Type=AWS::Glue::Table,Values=["*"]}]' &>/dev/null
            
            print_success "CloudTrailログ記録モジュールのデプロイが成功"
            return 0
        else
            print_error "CloudTrailの作成に失敗"
            return 1
        fi
    fi
    
    return 0
}

cloudtrail_logging_status() {
    print_info "CloudTrailログ記録モジュールの状態をチェック"
    
    local trail_status
    trail_status=$(aws cloudtrail get-trail-status --name "$CLOUDTRAIL_NAME" --query 'IsLogging' --output text 2>/dev/null)
    
    if [[ "$trail_status" == "True" ]]; then
        print_success "CloudTrailログ記録モジュールが正常動作中: $CLOUDTRAIL_NAME"
        
        # 追加情報を表示
        local latest_log
        latest_log=$(aws cloudtrail get-trail-status --name "$CLOUDTRAIL_NAME" --query 'LatestDeliveryTime' --output text 2>/dev/null)
        if [[ -n "$latest_log" && "$latest_log" != "None" ]]; then
            print_debug "✓ 最新ログ配信時間: $latest_log"
        fi
        
        # イベントセレクターをチェック
        local data_events
        data_events=$(aws cloudtrail get-event-selectors --trail-name "$CLOUDTRAIL_NAME" --query 'length(EventSelectors[0].DataResources)' --output text 2>/dev/null)
        print_debug "✓ データイベント設定数: $data_events"
        
        return 0
    elif [[ "$trail_status" == "False" ]]; then
        print_warning "CloudTrailが存在しますがログ記録が停止中"
        return 1
    else
        print_warning "CloudTrailログ記録モジュールが未デプロイ"
        return 1
    fi
}

cloudtrail_logging_cleanup() {
    print_info "CloudTrailログ記録モジュールのリソースを清理"
    
    if aws cloudtrail describe-trails --trail-name-list "$CLOUDTRAIL_NAME" &>/dev/null; then
        print_info "CloudTrailを削除: $CLOUDTRAIL_NAME"
        
        # ログ記録を停止
        aws cloudtrail stop-logging --name "$CLOUDTRAIL_NAME" &>/dev/null
        
        # CloudTrailを削除
        if aws cloudtrail delete-trail --name "$CLOUDTRAIL_NAME"; then
            print_success "CloudTrailログ記録モジュールの清理が成功"
            return 0
        else
            print_error "CloudTrailの削除に失敗"
            return 1
        fi
    else
        print_info "CloudTrailログ記録モジュールが未デプロイのため、清理不要"
        return 0
    fi
}

cloudtrail_logging_rollback() {
    print_info "CloudTrailログ記録モジュールの変更をロールバック"
    
    # CloudTrailのロールバックは削除と同じ
    cloudtrail_logging_cleanup
}

# =============================================================================
# ユーティリティ関数
# =============================================================================

show_cloudtrail_logs() {
    local hours="${1:-1}"
    
    print_info "過去${hours}時間のCloudTrailログを表示"
    
    local start_time end_time
    start_time=$(date -d "${hours} hours ago" -Iseconds)
    end_time=$(date -Iseconds)
    
    aws logs filter-log-events \
        --log-group-name CloudTrail/DataLakeEvents \
        --start-time "$(date -d "$start_time" +%s)000" \
        --end-time "$(date -d "$end_time" +%s)000" \
        --query 'events[].[eventTime,eventName,sourceIPAddress,userIdentity.type]' \
        --output table 2>/dev/null || \
    print_warning "CloudTrailログの取得に失敗、またはログが存在しません"
}

analyze_security_events() {
    print_info "セキュリティ関連イベントの分析"
    
    local start_time
    start_time=$(date -d "24 hours ago" -Iseconds)
    
    # 疑わしいアクティビティをチェック
    local suspicious_events=(
        "ConsoleLogin"
        "CreateUser"
        "AttachUserPolicy"
        "PutBucketPolicy"
        "DeleteBucket"
    )
    
    for event in "${suspicious_events[@]}"; do
        local count
        count=$(aws logs filter-log-events \
            --log-group-name CloudTrail/DataLakeEvents \
            --start-time "$(date -d "$start_time" +%s)000" \
            --filter-pattern "{ \$.eventName = $event }" \
            --query 'length(events)' \
            --output text 2>/dev/null || echo "0")
        
        if [[ "$count" -gt 0 ]]; then
            print_warning "⚠ セキュリティイベント検出: $event ($count 回)"
        else
            print_debug "✓ $event: イベントなし"
        fi
    done
}

# =============================================================================
# 直接実行される場合
# =============================================================================

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    # モジュールインターフェースをロード
    source "$SCRIPT_DIR/../../lib/interfaces/module_interface.sh"
    
    # 追加の分析コマンドをサポート
    case "${1:-}" in
        logs)
            load_config
            show_cloudtrail_logs "${2:-1}"
            exit 0
            ;;
        security)
            load_config
            analyze_security_events
            exit 0
            ;;
    esac
    
    # 渡された操作を実行
    if [[ $# -gt 0 ]]; then
        module_interface "$1" "cloudtrail_logging" "${@:2}"
    else
        echo "使用方法: $0 <action> [args...]"
        echo "利用可能な操作: validate, deploy, status, cleanup, rollback, logs, security"
    fi
fi