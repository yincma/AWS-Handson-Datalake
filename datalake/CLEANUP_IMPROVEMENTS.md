# 清理脚本改进说明

## 改进内容

### 🚀 自动发现功能
改进后的 `cleanup.sh` 脚本现在具备以下自动发现能力：

#### CloudFormation 堆栈自动发现
- **原功能**: 只删除预定义的堆栈名称
- **新功能**: 自动发现所有包含项目前缀的CloudFormation堆栈
- **优势**: 即使堆栈名称发生变化也能完全清理

#### S3 存储桶自动发现
- **原功能**: 只删除预定义的存储桶名称
- **新功能**: 自动发现所有包含项目前缀的S3存储桶
- **优势**: 包括临时创建的存储桶（如logs存储桶）

### 🔄 智能依赖管理

#### CloudFormation 堆栈删除顺序
```bash
删除顺序 (按依赖关系):
1. Lake Formation / Glue Catalog 相关堆栈
2. 其他应用堆栈
3. IAM 角色堆栈  
4. S3 存储堆栈 (最后删除)
```

#### 超时和重试机制
- **堆栈删除**: 30分钟超时，实时状态监控
- **存储桶删除**: 5次重试机制，智能版本清理
- **错误处理**: 失败时提供详细指导

### 🧹 增强的S3清理

#### 版本控制存储桶处理
```bash
自动检测和处理:
- 当前对象删除
- 历史版本删除  
- 删除标记清理
- 多轮清理重试
```

#### 批量删除优化
- 使用JSON格式批量操作
- 避免大量单独API调用
- 减少清理时间

### 📊 详细的进度报告

#### 实时状态监控
```bash
功能包括:
- 发现的资源数量统计
- 删除进度实时显示
- 每30秒状态更新
- 最终验证报告
```

#### 错误处理和建议
- 失败原因分析
- 手动清理指导
- AWS控制台链接提供

## 使用方法

### 基本清理
```bash
# 标准清理（需要确认）
./scripts/cleanup.sh

# 输入 'DELETE' 确认清理
```

### 高级选项
```bash
# 查看会删除什么资源（不执行删除）
./scripts/cleanup.sh --dry-run

# 强制清理（跳过确认，小心使用）
./scripts/cleanup.sh --force
```

## 清理过程

### 1. 资源发现阶段
```
[INFO] Discovering all CloudFormation stacks for project: dl-handson
[INFO] Found 3 CloudFormation stacks to delete
[INFO] Discovering all S3 buckets for project: dl-handson  
[INFO] Found 5 S3 buckets to delete
```

### 2. EMR集群终止
```
[INFO] Terminating EMR cluster: j-34J3AP28DYI6G
[INFO] Waiting for EMR clusters to terminate...
```

### 3. Glue资源清理
```
[INFO] Stopping crawler: dl-handson-raw-crawler
[INFO] Deleting crawler: dl-handson-raw-crawler
[INFO] Deleting table: customers
[INFO] Deleting Glue database: dl-handson_db
```

### 4. S3存储桶清理
```
[INFO] Processing S3 bucket: dl-handson-raw-dev
[INFO] Bucket dl-handson-raw-dev has versioning enabled, cleaning up all versions...
[INFO] Deleting 15 versions and 3 delete markers from dl-handson-raw-dev (attempt 1)
[INFO] Successfully deleted bucket: dl-handson-raw-dev
```

### 5. CloudFormation堆栈删除
```
[INFO] Deleting stacks in correct dependency order...
[INFO] Deleting CloudFormation stack: datalake-lake-formation-dev (status: CREATE_COMPLETE)
[INFO] Stack datalake-lake-formation-dev status: DELETE_IN_PROGRESS (45s elapsed)
[INFO] All CloudFormation stacks have been successfully deleted!
```

## 清理验证

### 最终检查
脚本会自动验证以下资源已完全删除：
- ✅ EMR 集群状态
- ✅ CloudFormation 堆栈  
- ✅ S3 存储桶
- ✅ Glue 资源
- ✅ CloudWatch 日志组

### 手动验证
如有必要，可在AWS控制台确认：
```bash
# 检查EMR集群
aws emr list-clusters --region us-east-1

# 检查CloudFormation堆栈  
aws cloudformation list-stacks --region us-east-1

# 检查S3存储桶
aws s3 ls | grep dl-handson

# 检查Glue资源
aws glue get-databases --region us-east-1
```

## 故障排除

### 常见问题

#### 1. S3存储桶删除失败
```
原因: 存储桶仍有未删除的版本
解决: 脚本会自动重试，或手动清理版本
```

#### 2. CloudFormation堆栈删除卡住
```
原因: 依赖关系或权限问题
解决: 脚本有30分钟超时，会提供手动删除指导
```

#### 3. 权限不足
```
原因: IAM权限不够
解决: 确保用户有AdministratorAccess或足够权限
```

### 手动清理指导

如果自动清理失败，按以下顺序手动删除：

1. **EMR集群** (最高优先级)
```bash
aws emr terminate-clusters --cluster-ids <cluster-id>
```

2. **S3存储桶** (高成本风险)
```bash 
aws s3 rm s3://bucket-name --recursive
aws s3api delete-bucket --bucket bucket-name
```

3. **CloudFormation堆栈**
```bash
aws cloudformation delete-stack --stack-name stack-name
```

## 成本节省

### 自动清理的成本效益
- ✅ **EMR集群**: 立即停止计费 (~$0.35/小时)
- ✅ **S3存储**: 删除所有对象和版本
- ✅ **其他服务**: Glue、Athena、CloudWatch等
- ✅ **预防意外费用**: 自动发现遗漏资源

### 预估节省
完整清理后的每日节省：
- EMR: $8.40/天
- S3: $0.01/天  
- 其他: $0.50/天
- **总计**: 约 $8.91/天

## 安全注意事项

### 删除确认
- 需要输入 'DELETE' 确认
- 显示将要删除的资源列表
- 提供取消选项

### 数据备份
⚠️ **重要**: 清理脚本会**永久删除**所有数据
- 确保重要数据已备份
- S3版本删除不可恢复
- CloudFormation删除不可回滚

## 总结

改进后的清理脚本提供：
- 🔍 **智能发现**: 自动找到所有相关资源
- ⚡ **高效清理**: 并行处理和批量操作
- 🛡️ **安全可靠**: 多重确认和错误处理
- 📊 **透明监控**: 详细进度和状态报告
- 💰 **成本控制**: 确保完全清理避免意外费用

使用这个改进的脚本，你可以放心地进行完整的环境清理，确保不会遗留任何产生费用的资源。