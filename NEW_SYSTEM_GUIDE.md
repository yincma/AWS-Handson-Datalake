# 新模块化系统使用指南

## 概述

新的模块化系统提供了更好的可维护性、一致性和扩展性。本指南将帮助您从旧系统迁移到新系统。

## 快速开始

### 1. 清理现有环境（如果需要）

```bash
# 查看现有资源
./scripts/cli/datalake status

# 清理所有资源
./scripts/cleanup.sh --force
```

### 2. 使用新系统部署

```bash
# 完整部署（推荐）
./scripts/cli/datalake deploy --full

# 或者分步部署
./scripts/cli/datalake deploy --module s3
./scripts/cli/datalake deploy --module iam
./scripts/cli/datalake deploy --module glue
./scripts/cli/datalake deploy --module lakeformation
```

### 3. 验证部署

```bash
# 检查系统状态
./scripts/cli/datalake status

# 验证数据湖功能
./scripts/cli/datalake validate
```

## 从旧系统迁移

如果您已经有使用旧系统部署的资源：

### 选项 1：自动迁移（推荐）

```bash
# 预览迁移计划
./scripts/migrate-to-new-system.sh --dry-run

# 执行迁移
./scripts/migrate-to-new-system.sh

# 迁移后清理旧资源
./scripts/migrate-to-new-system.sh --cleanup-old
```

### 选项 2：全新部署

```bash
# 1. 备份重要数据
aws s3 sync s3://your-old-bucket ./backup/

# 2. 删除旧资源
./scripts/cleanup.sh --force

# 3. 使用新系统部署
./scripts/cli/datalake deploy --full

# 4. 恢复数据
aws s3 sync ./backup/ s3://your-new-bucket
```

## 主要改进

### 1. 统一的命名约定
- 所有资源使用一致的命名格式：`${PROJECT_PREFIX}-stack-${MODULE}-${ENVIRONMENT}`
- 自动处理依赖关系

### 2. 模块化架构
- 每个组件独立管理
- 清晰的依赖关系
- 更容易调试和维护

### 3. 改进的CLI
```bash
# 部署特定模块
./scripts/cli/datalake deploy --module s3

# 查看详细状态
./scripts/cli/datalake status --detailed

# 运行验证
./scripts/cli/datalake validate

# 查看成本
./scripts/cli/datalake cost

# 销毁资源
./scripts/cli/datalake destroy
```

## 配置

### 基本配置
配置文件位于 `configs/config.local.env`：

```bash
# 项目配置
PROJECT_PREFIX=dl-handson-v2
ENVIRONMENT=dev
AWS_REGION=us-east-1
```

### 高级配置
```bash
# EMR配置
EMR_INSTANCE_TYPE=m5.xlarge
EMR_INSTANCE_COUNT=3

# 监控配置
ENABLE_MONITORING=true
ALERT_EMAIL=your-email@example.com
```

## 故障排除

### 问题：堆栈名称不匹配
**症状**：部署失败，提示找不到导出值

**解决方案**：
```bash
# 运行迁移脚本
./scripts/migrate-to-new-system.sh
```

### 问题：权限错误
**症状**：CloudFormation 创建失败

**解决方案**：
```bash
# 验证AWS权限
aws sts get-caller-identity

# 确保有必要的权限
# - CloudFormation
# - S3
# - IAM
# - Glue
# - Lake Formation
```

### 问题：依赖错误
**症状**：模块部署顺序错误

**解决方案**：
```bash
# 使用正确的部署顺序
./scripts/cli/datalake deploy --module s3
./scripts/cli/datalake deploy --module iam
./scripts/cli/datalake deploy --module glue
./scripts/cli/datalake deploy --module lakeformation

# 或使用 --full 自动处理
./scripts/cli/datalake deploy --full
```

## 最佳实践

1. **始终使用CLI**：避免直接调用模块脚本
2. **定期验证**：运行 `datalake validate` 检查系统健康
3. **监控成本**：使用 `datalake cost` 跟踪支出
4. **备份配置**：定期备份 `configs/` 目录

## 支持

如有问题，请查看：
- 项目文档：`docs/`
- 日志文件：`logs/`
- 运行诊断：`./scripts/cli/datalake diagnose`