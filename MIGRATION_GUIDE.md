# 数据湖系统迁移指南

## 概述

本指南说明如何从旧的数据湖系统安全迁移到新的模块化系统。

### 主要变化

1. **命名约定**
   - 旧系统: `datalake-<module>-<env>` (如 `datalake-s3-storage-dev`)
   - 新系统: `${PROJECT_PREFIX}-stack-<module>-<env>` (如 `dl-handson-stack-s3-dev`)

2. **架构改进**
   - 旧系统: 串行部署脚本
   - 新系统: 模块化并行部署，支持依赖管理

3. **CLI工具**
   - 旧系统: 多个独立脚本（setup-env.sh, cleanup.sh等）
   - 新系统: 统一的CLI工具 `datalake`

## 迁移前准备

### 1. 检查当前状态

```bash
# 检查当前系统模式
./scripts/datalake-compat mode

# 检查现有资源
./scripts/datalake-compat status
```

### 2. 备份重要数据

虽然迁移过程会自动备份，但建议手动备份关键数据：

```bash
# 备份S3数据（如果需要）
aws s3 sync s3://dl-handson-raw-dev ./backup/s3/raw/
aws s3 sync s3://dl-handson-clean-dev ./backup/s3/clean/
aws s3 sync s3://dl-handson-analytics-dev ./backup/s3/analytics/
```

## 迁移步骤

### 方法一：使用迁移脚本（推荐）

```bash
# 1. 执行迁移（会自动备份）
./scripts/migrate-to-modular.sh

# 2. 验证迁移结果
./scripts/cli/datalake status

# 3. 如果需要回滚
./scripts/rollback-migration.sh /path/to/backup/directory
```

### 方法二：使用兼容性CLI

```bash
# 1. 确认当前在旧系统模式
./scripts/datalake-compat mode

# 2. 执行迁移
./scripts/datalake-compat migrate

# 3. 验证结果
./scripts/datalake-compat status
```

### 方法三：手动迁移（高级用户）

如果需要更精细的控制，可以逐个模块迁移：

```bash
# 1. 设置迁移模式
export MIGRATION_MODE=true

# 2. 按顺序迁移各模块
./scripts/migrate-to-modular.sh --module s3_storage
./scripts/migrate-to-modular.sh --module iam_roles
./scripts/migrate-to-modular.sh --module glue_catalog
./scripts/migrate-to-modular.sh --module lake_formation
./scripts/migrate-to-modular.sh --module cost_monitoring

# 3. 取消迁移模式
unset MIGRATION_MODE
```

## 迁移后验证

### 1. 检查所有模块状态

```bash
./scripts/cli/datalake status
```

### 2. 验证S3桶访问

```bash
# 列出S3桶内容
aws s3 ls s3://dl-handson-raw-dev/
aws s3 ls s3://dl-handson-clean-dev/
aws s3 ls s3://dl-handson-analytics-dev/
```

### 3. 验证IAM角色

```bash
# 检查角色是否存在
aws iam get-role --role-name dl-handson-DataEngineerRole-dev
aws iam get-role --role-name dl-handson-AnalystRole-dev
```

### 4. 验证Glue Catalog

```bash
# 检查数据库
aws glue get-database --name dl_handson_db

# 检查爬虫
aws glue get-crawler --name dl-handson-raw-crawler
aws glue get-crawler --name dl-handson-clean-crawler
```

## 使用新系统

### 基本命令

```bash
# 部署
./scripts/cli/datalake deploy

# 查看状态
./scripts/cli/datalake status

# 部署特定模块
./scripts/cli/datalake module deploy s3_storage

# 查看帮助
./scripts/cli/datalake help
```

### 高级功能

```bash
# 并行部署（使用优化的编排器）
./scripts/core/deployment/parallel_orchestrator.sh deploy

# 成本分析
./scripts/cli/datalake costs

# 安全审计
./scripts/cli/datalake security
```

## 兼容性支持

在过渡期间，可以使用兼容性CLI来自动选择合适的系统：

```bash
# 自动检测并使用正确的系统
./scripts/datalake-compat deploy
./scripts/datalake-compat destroy
./scripts/datalake-compat status
```

### 强制使用特定系统

```bash
# 强制使用新系统
./scripts/datalake-compat --force-new deploy

# 强制使用旧系统
./scripts/datalake-compat --force-legacy deploy

# 切换默认模式
./scripts/datalake-compat switch new    # 切换到新系统
./scripts/datalake-compat switch legacy  # 切换到旧系统
```

## 故障排除

### 问题1：迁移失败

如果迁移过程中断：

1. 检查错误日志
2. 使用备份回滚：
   ```bash
   ./scripts/rollback-migration.sh /path/to/backup
   ```
3. 修复问题后重试

### 问题2：堆栈名称冲突

如果遇到堆栈名称冲突：

```bash
# 检查现有堆栈
aws cloudformation list-stacks --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE

# 删除冲突的堆栈（谨慎操作）
aws cloudformation delete-stack --stack-name <stack-name>
```

### 问题3：S3桶无法删除

S3桶包含数据时无法删除：

```bash
# 清空桶（谨慎！）
aws s3 rm s3://bucket-name --recursive

# 如果有版本控制
./scripts/utils/delete-s3-versions.py
```

## 最佳实践

1. **先在测试环境验证**：在生产环境迁移前，先在开发/测试环境完成迁移

2. **分阶段迁移**：可以先迁移非关键模块，验证后再迁移核心模块

3. **保留备份**：迁移成功后，建议保留备份至少一周

4. **更新文档**：迁移后更新团队文档和运维手册

5. **培训团队**：确保团队成员熟悉新的CLI和命令

## 迁移完成后

1. **删除旧脚本引用**：更新所有自动化脚本，使用新的CLI

2. **更新CI/CD**：如果有自动化部署流程，更新为使用新系统

3. **监控和优化**：利用新系统的监控功能，优化资源使用

## 支持

如有问题，请：

1. 查看日志文件：`logs/deployment/`
2. 检查系统状态：`./scripts/cli/datalake status`
3. 查看详细调试信息：`LOG_LEVEL=DEBUG ./scripts/cli/datalake status`