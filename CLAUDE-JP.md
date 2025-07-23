# CLAUDE.md

このファイルは、このリポジトリでコードを操作する際にClaude Code (claude.ai/code) に対するガイダンスを提供します。

## プロジェクト概要

これは、多層アプローチ（Raw → Clean → Analytics）を使用してエンタープライズレベルのデータレイクアーキテクチャを実証するAWSデータレイクハンズオンプロジェクトです。プロジェクトは主にBashスクリプト、Python（PySpark）、SQL、およびCloudFormation YAMLテンプレートで書かれています。

## 主要な開発コマンド

### メインデプロイメントコマンド
```bash
# 🆕 推奨: 統一CLIを使用（v2.1+）
./scripts/cli/datalake deploy              # 基本インフラストラクチャのみ
./scripts/cli/datalake deploy --full       # EMRクラスターと分析を含む完全デプロイメント
./scripts/cli/datalake analytics           # PySpark分析ジョブを実行

# 従来のスクリプト（フォールバックのみ）
# ./scripts/deploy-all.sh
# ./scripts/deploy-all.sh --with-emr --with-analytics
```

### リソース管理
```bash
# 🆕 推奨: 統一CLIを使用
./scripts/cli/datalake status              # システム状態確認
./scripts/cli/datalake costs               # コスト分析
./scripts/cli/datalake destroy --force --deep-clean  # 完全クリーンアップ

# 従来のスクリプト（フォールバックのみ）
# ./scripts/cost-optimization.sh
# ./scripts/cleanup.sh --force --deep-clean --retry-failed
```

### 設定管理
```bash
# 🆕 推奨: 統一CLIを使用
./scripts/cli/datalake config              # 設定表示・検証
./scripts/cli/datalake validate            # 設定検証

# 手動設定管理
cp configs/config.env configs/config.local.env
source configs/env-vars.sh
```

## アーキテクチャ概要

![AWS Data Lake Architecture](./Arch.drawio.svg)

### データフローパイプライン
1. **Raw層**（ブロンズ）: S3バケットが生のCSVデータを受信
2. **Clean層**（シルバー）: Glue CrawlerとDataBrewを介して処理
3. **Analytics層**（ゴールド）: EMR PySparkが集約データセットを作成
4. **クエリ層**: Amazon AthenaでSQL分析を有効化

### コアAWSサービス
- **ストレージ**: Amazon S3（3層階層）
- **カタログ**: AWS Glue（データカタログ、クローラー、DataBrew）
- **処理**: Amazon EMR（PySparkジョブ）
- **分析**: Amazon Athena（サーバーレスSQL）
- **ガバナンス**: AWS Lake Formation（権限、セキュリティ）
- **インフラストラクチャ**: CloudFormation（IaCテンプレート）

### 主要コンポーネント
- **設定**: `configs/config.env` - 一元化されたプロジェクト設定
- **インフラストラクチャ**: `templates/*.yaml` - CloudFormationテンプレート
- **スクリプト**: `scripts/` - デプロイメント自動化とユーティリティ
- **分析**: `scripts/pyspark_analytics.py` - PySparkデータ処理
- **サンプルデータ**: `sample-data/` - Eコマースデータセット（顧客、注文、製品、注文アイテム）

## 重要なファイル場所

### 設定ファイル
- `configs/config.env` - メイン設定テンプレート
- `configs/config.local.env` - ローカルオーバーライド（ユーザーが作成）
- `configs/env-vars.sh` - 自動生成された環境変数

### コアスクリプト
- `scripts/deploy-all.sh` - マスターデプロイメントオーケストレーター
- `scripts/setup-env.sh` - ベースインフラストラクチャデプロイメント
- `scripts/cleanup.sh` - 完全なリソースクリーンアップ
- `scripts/cost-optimization.sh` - コスト監視と最適化
- `scripts/pyspark_analytics.py` - メインデータ処理ロジック

### インフラストラクチャテンプレート
- `templates/s3-storage-layer.yaml` - S3バケットとライフサイクルポリシー
- `templates/iam-roles-policies.yaml` - IAMロールと権限
- `templates/lake-formation-simple.yaml` - プライマリLake Formationセットアップ
- `templates/glue-catalog.yaml` - Glueデータベースとクローラー

### 分析リソース
- `scripts/athena_queries.sql` - サンプルSQLクエリとテーブル定義
- `scripts/utils/table_schemas.json` - Glueテーブルスキーマ定義

## 開発プラクティス

### 設定管理
- ローカルカスタマイゼーションには常に`configs/config.local.env`を使用
- AWS認証情報などの機密情報をコミットしない
- デプロイスクリプトは、指定されていない場合にEC2キーペアとVPCサブネットを自動発見する

### リソース命名規則
- 形式: `${PROJECT_PREFIX}-${RESOURCE_TYPE}-${ENVIRONMENT}`
- 例: `dl-handson-raw-dev`（S3バケット）
- デフォルトプレフィックス: `dl-handson`、環境: `dev`

### コスト最適化
- EMRクラスターが主要なコストドライバー（コストの約70-80%）
- 実験後は常にクリーンアップスクリプトを実行: `./scripts/cleanup.sh --force --deep-clean --retry-failed`
- EMRにSpotインスタンスを使用してコストを60-70%削減
- `--deep-clean`を有効にしてS3バージョニングオブジェクトと削除マーカーを削除

### エラー回復
- 失敗したCloudFormationスタックは`--retry-failed`オプションで再試行可能
- Lake Formationリソースはスタック削除失敗を引き起こす可能性があります - 再試行メカニズムがこれらを処理します
- `./scripts/utils/check-resources.sh`でリソースステータスを確認

## テストと検証

### データ品質検証
```sql
-- Athenaでの基本データ検証
SELECT COUNT(*) FROM "dl-handson-db".customers;
SELECT * FROM "dl-handson-db".orders LIMIT 10;
```

### リソース検証
```bash
# S3バケットを確認
aws s3 ls | grep dl-handson

# Glueデータベースを確認
aws glue get-databases

# EMRクラスターを確認
aws emr list-clusters --active
```

## 一般的な問題と解決策

### EMRアクセス
- EC2キーペアは自動発見または作成される
- 秘密鍵ファイル（*.pem）は作成時にローカルに保存される
- デフォルトVPCサブネットが自動的に使用される

### Lake Formation権限
- プロジェクトは簡素化されたセットアップのためにサービス連携ロールを使用
- サービス連携ロールが失敗した場合はカスタムロールにフォールバック
- `lake-formation-simple.yaml`がプライマリテンプレート

### S3クリーンアップ
- バージョニングオブジェクトを削除するために常に`--deep-clean`を使用
- S3ライフサイクルポリシーがオブジェクトの移行を自動管理
- 削除マーカーには明示的なクリーンアップが必要

## セキュリティ考慮事項

- IAMロールは最小権限の原則に従う
- Lake Formationは細粒度のデータアクセス制御を提供
- S3バケットはデフォルトで暗号化が有効
- 監査証跡のためにCloudTrailログが統合されている

## サンプルデータスキーマ

プロジェクトにはEコマースサンプルデータが含まれています：
- **customers**: 顧客プロファイルとセグメント
- **products**: カテゴリと価格を含む製品カタログ
- **orders**: ステータスと金額を含む注文トランザクション
- **order_items**: 数量と価格を含む明細アイテム詳細

この複数テーブルスキーマにより、データレイク層全体での複雑な分析クエリと結合が可能になります。