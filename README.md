# AWS データレイク総合実践プロジェクト v2.1

**作者: mayinchen**

## プロジェクト概要

本プロジェクトは、AWSクラウドサービスを基盤としたエンタープライズレベルのデータレイクプラットフォームをゼロから構築するための実践的なハンズオンプロジェクトです。**v2.1では最適化されたモジュラーアーキテクチャと統一されたCLI管理システムを提供します。**

多層アーキテクチャ設計（Raw → Clean → Analytics）により、データの収集、保存、変換、分析の完全なデータ処理パイプラインを実現しています。

## 🆕 v2.1 新機能ハイライト

- **統一CLI管理**: `datalake` コマンドによる一元化されたシステム管理
- **モジュラーアーキテクチャ**: 高度に独立したコンポーネント設計と並列デプロイメントオーケストレーター
- **簡素化された設定**: Lake Formation Simpleモードによる権限管理の簡易化
- **エンタープライズレベルの信頼性**: 包括的なエラーハンドリングとリトライロジック
- **高度な監視機能**: CloudTrail統合セキュリティ監視とコスト最適化
- **自動化されたデプロイ**: 依存関係を考慮したインテリジェントなリソース管理
- **Eコマース分析**: 専用のEコマースデータ処理・分析モジュール
- **並列オーケストレーション**: インテリジェントな並列実行によるデプロイ時間の最適化

## 目次
- [技術アーキテクチャ](#技術アーキテクチャ)
- [前提条件](#前提条件)
- [クイックスタート](#クイックスタート)
- [統一CLI使用ガイド](#統一cli使用ガイド)
- [システム構成](#システム構成)
- [モジュール詳細](#モジュール詳細)
- [運用管理](#運用管理)
- [トラブルシューティング](#トラブルシューティング)

## 技術アーキテクチャ

![AWS Data Lake Architecture](./Arch.drawio.svg)

### コアサービススタック
- **ストレージ層**: Amazon S3 (3層ストレージ + ライフサイクル管理)
- **データカタログ**: AWS Glue (Crawler + データカタログ)
- **データガバナンス**: AWS Lake Formation (簡素化された権限制御)
- **計算エンジン**: Amazon EMR (Spark分散処理)
- **分析エンジン**: Amazon Athena (サーバーレス SQL クエリ)
- **監視・コスト管理**: CloudTrail + AWS Budgets + CloudWatch

### データフローアーキテクチャ

<div align="center">

#### 🌊 **データレイク3層アーキテクチャ**

</div>

```mermaid
graph TB
    %% スタイル定義
    classDef rawStyle fill:#FFE5CC,stroke:#FF8C42,stroke-width:3px,color:#000
    classDef cleanStyle fill:#CCE5FF,stroke:#4285F4,stroke-width:3px,color:#000
    classDef analyticsStyle fill:#D4EDDA,stroke:#28A745,stroke-width:3px,color:#000
    classDef catalogStyle fill:#F8F9FA,stroke:#6C757D,stroke-width:2px,color:#000
    classDef processStyle fill:#E7E7E7,stroke:#495057,stroke-width:2px,color:#000
    
    %% データソース
    subgraph sources["📥 データソース"]
        DS1[CSVファイル]
        DS2[JSONファイル]
        DS3[ログファイル]
    end
    
    %% ストレージ層
    subgraph storage["🗄️ ストレージ層"]
        RAW["🥉 Raw層 (Bronze)<br/>生データ保存<br/>S3: dl-handson-v2-raw-dev"]:::rawStyle
        CLEAN["🥈 Clean層 (Silver)<br/>クレンジング済みデータ<br/>S3: dl-handson-v2-clean-dev"]:::cleanStyle
        ANALYTICS["🥇 Analytics層 (Gold)<br/>分析用集計データ<br/>S3: dl-handson-v2-analytics-dev"]:::analyticsStyle
    end
    
    %% カタログ層
    CATALOG["📚 AWS Glue データカタログ<br/>統合メタデータ管理"]:::catalogStyle
    
    %% 処理層
    subgraph processing["⚙️ 処理と分析"]
        CRAWLER["🔍 Glue Crawler<br/>自動スキーマ検出"]:::processStyle
        DATABREW["🧹 Glue DataBrew<br/>データクレンジング"]:::processStyle
        EMR["⚡ EMR + Spark<br/>大規模データ処理"]:::processStyle
        ATHENA["📊 Amazon Athena<br/>SQLクエリ分析"]:::processStyle
    end
    
    %% ガバナンス
    LAKEFORMATION["🛡️ Lake Formation<br/>アクセス権限管理"]:::processStyle
    
    %% データフロー
    DS1 --> RAW
    DS2 --> RAW
    DS3 --> RAW
    
    RAW -->|ETL処理| CLEAN
    CLEAN -->|変換・集計| ANALYTICS
    
    RAW -.->|メタデータ登録| CATALOG
    CLEAN -.->|メタデータ登録| CATALOG
    ANALYTICS -.->|メタデータ登録| CATALOG
    
    CATALOG <--> CRAWLER
    CATALOG <--> DATABREW
    CATALOG <--> EMR
    CATALOG <--> ATHENA
    
    CATALOG <--> LAKEFORMATION
    
    %% 注釈
    RAW -.- crawlerNote["定期的に<br/>自動スキャン"]
    CLEAN -.- databrewNote["データ品質<br/>ルール適用"]
    ANALYTICS -.- emrNote["ビジネス<br/>ロジック実行"]
```

<div align="center">

#### 📋 **データ処理パイプライン詳細**

</div>

| 🏷️ **ステージ** | 📂 **レイヤー** | 📝 **説明** | 💾 **ストレージ** | 🔧 **処理ツール** | ⏱️ **頻度** |
|:---:|:---:|:---|:---|:---|:---:|
| **1️⃣ 収集** | Raw<br/>(Bronze) | 様々なソースからの生データをそのまま保存 | `s3://dl-handson-v2-raw-dev/`<br/>`└── landing/`<br/>`    └── ecommerce/` | S3 Transfer<br/>Kinesis Firehose | リアルタイム |
| **2️⃣ 検証** | Raw → Clean | スキーマ検出とデータ品質チェック | Glue Data Catalog | Glue Crawler<br/>Data Quality | 1時間毎 |
| **3️⃣ 変換** | Clean<br/>(Silver) | データクレンジング、正規化、重複排除 | `s3://dl-handson-v2-clean-dev/`<br/>`└── processed/`<br/>`    └── ecommerce/` | Glue DataBrew<br/>Glue ETL | 日次 |
| **4️⃣ 集計** | Analytics<br/>(Gold) | ビジネスメトリクス計算、KPI生成 | `s3://dl-handson-v2-analytics-dev/`<br/>`└── aggregated/`<br/>`    └── reports/` | EMR Spark<br/>PySpark Job | 日次/週次 |
| **5️⃣ 分析** | Query Layer | アドホック分析とレポート作成 | Athena Query Results | Amazon Athena<br/>QuickSight | オンデマンド |

<div align="center">

#### 🎯 **主要コンポーネントの詳細**

</div>

<table>
<tr>
<td width="50%">

**📊 データ管理コンポーネント**

| コンポーネント | 役割 |
|:---|:---|
| 🔍 **Glue Crawler** | • 新規データの自動検出<br/>• スキーマの自動推論<br/>• パーティション管理 |
| 📚 **Glue Data Catalog** | • 統一メタデータストア<br/>• テーブル定義管理<br/>• データ系譜追跡 |
| 🛡️ **Lake Formation** | • 細粒度アクセス制御<br/>• データマスキング<br/>• 監査ログ管理 |

</td>
<td width="50%">

**⚡ 処理・分析コンポーネント**

| コンポーネント | 役割 |
|:---|:---|
| 🧹 **Glue DataBrew** | • ビジュアルデータ準備<br/>• 250+の変換機能<br/>• データプロファイリング |
| ⚡ **EMR + Spark** | • 大規模並列処理<br/>• 機械学習パイプライン<br/>• ストリーミング処理 |
| 📊 **Amazon Athena** | • サーバーレスSQL分析<br/>• 標準SQL準拠<br/>• 結果キャッシュ機能 |

</td>
</tr>
</table>

<div align="center">

#### 🔄 **データライフサイクル管理**

</div>

```mermaid
graph LR
    subgraph lifecycle["📅 S3ライフサイクルポリシー"]
        HOT["🔥 ホット<br/>0-30日<br/>Standard"]
        WARM["🌡️ ウォーム<br/>31-90日<br/>Standard-IA"]
        COLD["❄️ コールド<br/>91-365日<br/>Glacier IR"]
        ARCHIVE["🗄️ アーカイブ<br/>365日+<br/>Deep Archive"]
        
        HOT -->|30日後| WARM
        WARM -->|90日後| COLD
        COLD -->|365日後| ARCHIVE
    end
    
    style HOT fill:#FFE5CC,stroke:#FF8C42
    style WARM fill:#FFF3CD,stroke:#FFC107
    style COLD fill:#CCE5FF,stroke:#4285F4
    style ARCHIVE fill:#E7E7E7,stroke:#6C757D
```

## 前提条件

- AWS CLIがインストール済み
- AWS認証情報が設定済み (`aws configure`)
- Bash 4.0以上
- Python 3.8以上（EMR分析ジョブ用）
- 適切なIAM権限（管理者権限推奨）

## クイックスタート

### 1. 環境準備
```bash
# プロジェクトディレクトリに移動
cd /Users/umatoratatsu/Documents/AWS/AWS-Handson/Datalake/git

# 設定ファイルをカスタマイズ（オプション）
cp configs/config.env configs/config.local.env
# config.local.envを編集してプロジェクト設定を調整
```

### 2. 環境変数の設定
```bash
# 設定ファイルを読み込む
source configs/config.env

# 環境変数を確認
echo "PROJECT_PREFIX=$PROJECT_PREFIX"  # dl-handson-v2
echo "ENVIRONMENT=$ENVIRONMENT"        # dev
```

### 3. 基本デプロイメント
```bash
# 基本インフラストラクチャのみデプロイ
./scripts/cli/datalake deploy
```

### 4. 完全デプロイメント（EMR + Analytics）
```bash
# EMRクラスターと分析ジョブを含む完全デプロイ
./scripts/cli/datalake deploy --full
```

### 5. システム確認
```bash
# システム全体の状態確認
./scripts/cli/datalake status

# デプロイされたリソースの確認
./scripts/utils/check-resources.sh
```

## 統一CLI使用ガイド

### 基本コマンド

```bash
# ヘルプ表示
./scripts/cli/datalake help

# バージョン確認
./scripts/cli/datalake version

# システム状態確認
./scripts/cli/datalake status

# 設定検証
./scripts/cli/datalake validate
```

### デプロイメントコマンド

```bash
# 基本デプロイ（S3、IAM、Glue、Lake Formation）
./scripts/cli/datalake deploy

# インフラストラクチャのみデプロイ
./scripts/cli/datalake infrastructure deploy

# 監視モジュールのデプロイ
./scripts/cli/datalake monitoring deploy

# 完全デプロイ（全モジュール）
./scripts/cli/datalake deploy --full
```

### モジュール管理

```bash
# 個別モジュールの操作
./scripts/cli/datalake module <action> <module_name>
# actions: validate, deploy, status, cleanup, rollback
# modules: s3_storage, iam_roles, glue_catalog, lake_formation,
#          emr_cluster, cost_monitoring, cloudtrail_logging

# 例：
./scripts/cli/datalake module deploy s3_storage
./scripts/cli/datalake module status emr_cluster
```

### 監視・分析

```bash
# コスト分析
./scripts/cli/datalake costs

# CloudTrailログ確認（過去N時間）
./scripts/cli/datalake logs --hours 1

# セキュリティイベント分析
./scripts/cli/datalake security

# システム監視
./scripts/cli/datalake monitoring
```

### クリーンアップ

```bash
# 🆕 推奨: 統一CLIを使用
# 通常削除（確認プロンプト付き）
./scripts/cli/datalake destroy

# 完全削除（S3バージョンオブジェクトも削除）
./scripts/cli/datalake destroy --force --deep-clean
```

## システム構成

### 最適化されたモジュール構成

```bash
scripts/
├── cli/
│   └── datalake                    # 統一CLI管理ツール v2.0.0
├── core/                           # コアモジュール
│   ├── infrastructure/
│   │   ├── s3_storage.sh          # S3ストレージ管理
│   │   └── iam_roles.sh           # IAMロール管理
│   ├── catalog/
│   │   ├── glue_catalog.sh        # Glueデータカタログ
│   │   └── lake_formation.sh      # Lake Formation権限管理
│   ├── compute/
│   │   └── emr_cluster.sh         # EMRクラスター管理
│   ├── data_processing/
│   │   └── ecommerce_analytics.py # Eコマース分析処理
│   ├── monitoring/
│   │   ├── cost_monitoring.sh     # コスト監視
│   │   └── cloudtrail_logging.sh  # セキュリティ監査
│   └── deployment/
│       └── parallel_orchestrator.sh # 並列デプロイオーケストレーター
├── lib/                            # 共有ライブラリ
│   ├── common.sh                   # 共通ユーティリティ v2.0.0
│   ├── config/
│   │   └── validator.sh           # 設定検証
│   ├── interfaces/
│   │   └── module_interface.sh    # モジュールインターフェース
│   └── monitoring/
│       ├── monitor.sh             # 監視機能
│       └── tracer.py             # トレース機能
└── utils/                          # ユーティリティツール
    ├── check-resources.sh          # リソース確認
    ├── delete-s3-versions.py      # S3バージョン削除
    ├── create_glue_tables.py      # Glueテーブル作成
    └── table_schemas.json          # テーブルスキーマ定義
```

### CloudFormationテンプレート

```bash
templates/
├── s3-storage-layer.yaml          # S3 3層ストレージ設定
├── iam-roles-policies.yaml        # IAMロールとポリシー
├── glue-catalog.yaml              # Glueデータカタログ
├── lake-formation-simple.yaml     # 簡素化Lake Formation
└── cost-monitoring.yaml           # コスト監視設定
```

## モジュール詳細

### 1. S3 Storage Module
- **機能**: 3層データレイクストレージ（Raw/Clean/Analytics）
- **バケット名**: 
  - `${PROJECT_PREFIX}-raw-${ENVIRONMENT}`
  - `${PROJECT_PREFIX}-clean-${ENVIRONMENT}`
  - `${PROJECT_PREFIX}-analytics-${ENVIRONMENT}`
- **特徴**: ライフサイクル管理、暗号化、バージョニング対応

### 2. IAM Roles Module  
- **機能**: 最小権限の原則に基づくロール設定
- **主要ロール**:
  - GlueServiceRole: Glueクローラー用
  - EMRServiceRole: EMRクラスター用
  - LakeFormationServiceRole: データガバナンス用

### 3. Glue Catalog Module
- **機能**: データカタログとメタデータ管理
- **データベース**: `${PROJECT_PREFIX}-db`
- **テーブル**: customers, products, orders, order_items

### 4. Lake Formation Module (Simplified)
- **機能**: 簡素化されたデータ権限制御
- **特徴**: サービス連携ロール使用、自動権限設定

### 5. EMR Cluster Module
- **機能**: Sparkベースの分散データ処理
- **クラスター名**: `${PROJECT_PREFIX}-cluster-${ENVIRONMENT}`
- **デフォルト構成**: Master (m5.xlarge) x 1, Core (m5.xlarge) x 2

### 6. Cost Monitoring Module
- **機能**: リアルタイムコスト監視と予算アラート
- **監視対象サービス**:
  - Amazon EMR
  - Amazon S3
  - AWS Glue
  - Amazon Athena
  - AWS Lake Formation

### 7. CloudTrail Logging Module
- **機能**: セキュリティ監査とコンプライアンス
- **Trail名**: `${PROJECT_PREFIX}-cloudtrail-${ENVIRONMENT}`

### 8. Eコマース分析モジュール 🆕
- **機能**: 専用のEコマースデータ処理と分析
- **分析内容**:
  - 顧客行動分析
  - 商品売上分析
  - 注文トレンド分析
  - 収益レポート

## 運用管理

### 日常的な監視
```bash
# システムヘルスチェック
./scripts/cli/datalake status

# コスト監視
./scripts/cli/datalake costs

# リソース使用状況確認
aws s3 ls s3://${PROJECT_PREFIX}-raw-${ENVIRONMENT} --recursive --summarize
```

### データ分析の実行
```bash
# サンプルデータのアップロード（初回のみ）
./scripts/cli/datalake upload --sample-data

# Glueクローラーの実行
aws glue start-crawler --name ${PROJECT_PREFIX}-raw-crawler

# Eコマース分析の実行
./scripts/cli/datalake analytics

# Athenaクエリの実行
./scripts/cli/datalake query "SELECT * FROM customers LIMIT 10"
```

### データ処理パイプライン
```bash
# 1. データ収集
aws s3 cp data/ s3://${PROJECT_PREFIX}-raw-${ENVIRONMENT}/landing/ecommerce/ --recursive

# 2. データカタログ更新
aws glue start-crawler --name ${PROJECT_PREFIX}-raw-crawler

# 3. データクレンジング（DataBrew使用）
# AWSコンソールまたはAPIでDataBrewジョブを実行

# 4. データ分析（EMR + Spark）
./scripts/submit_pyspark_job.sh

# 5. クエリ分析（Athena）
aws athena start-query-execution \
  --query-string "SELECT * FROM analytics_db.sales_summary" \
  --result-configuration "OutputLocation=s3://${PROJECT_PREFIX}-analytics-${ENVIRONMENT}/athena-results/"
```

## トラブルシューティング

### よくある問題と解決法

#### 1. 環境変数エラー
```bash
# 問題: "必須環境変数が不足"
# 解決: 設定ファイルを読み込む
source configs/config.env
```

#### 2. CloudFormationスタックエラー
```bash
# 問題: "Stack already exists"
# 解決: 既存スタックを削除してから再デプロイ
aws cloudformation delete-stack --stack-name <stack-name>
aws cloudformation wait stack-delete-complete --stack-name <stack-name>
```

#### 3. EMRクラスター接続エラー
```bash
# 問題: "Cannot connect to EMR cluster"
# 解決: セキュリティグループとキーペアを確認
./scripts/cli/datalake module status emr_cluster
```

#### 4. Cost Monitoring デプロイエラー
```bash
# 問題: "Budget creation failed"
# 解決: cost-monitoring.yamlのCostFiltersを修正済み
# Service維度フィルターを使用するように更新されています
```

#### 5. 権限不足エラー
```bash
# 問題: "Access Denied"
# 解決: IAMロールとLake Formation権限を確認
./scripts/cli/datalake module deploy iam_roles
./scripts/cli/datalake module deploy lake_formation
```

### デバッグモード
```bash
# 詳細ログを有効化
export DEBUG=true
export LOG_LEVEL=DEBUG

# デバッグモードで実行
./scripts/cli/datalake status

# モジュールログの確認
tail -f logs/datalake-*.log
```

## 予想コストと推奨事項

### 月間コスト見積もり（東京リージョン）
- **基本構成（EMRなし）**: $5-15/月
- **EMR含む構成**: $50-200/月（使用時間による）
- **ストレージ**: $1-5/月（データ量による）

### コスト最適化のヒント
1. EMRクラスターは使用後すぐに削除
2. S3ライフサイクルポリシーの活用（自動設定済み）
3. Spotインスタンスの使用でEMRコストを60-70%削減
4. 定期的なコスト監視レポートの確認
5. 並列デプロイオーケストレーターを使用してデプロイ時間を短縮

## セキュリティベストプラクティス

- IAMロールは最小権限の原則に従って設定
- S3バケットは暗号化とバージョニングを有効化
- Lake Formationによる細粒度のアクセス制御
- CloudTrailによる全操作の監査ログ記録
- VPCエンドポイントの使用を推奨
- 定期的なセキュリティイベント分析
- 機密データの自動マスキング処理

## プロジェクトの特徴

### 🎯 コアの強み
1. **モジュラー設計**: 各コンポーネントが独立してデプロイ可能
2. **並列デプロイ**: インテリジェントな依存関係解決による高速デプロイ
3. **Eコマースシナリオ**: 組み込みのEコマースデータモデルと分析例
4. **中国語サポート**: コード内の完全な中国語コメントとドキュメント
5. **コスト最適化**: 自動化されたコスト監視と最適化の推奨事項

### 📊 データモデル
- **顧客テーブル** (customers): 顧客プロファイル情報
- **商品テーブル** (products): 商品カタログデータ
- **注文テーブル** (orders): 注文取引記録
- **注文明細テーブル** (order_items): 注文商品明細

## ライセンス

このプロジェクトはMITライセンスの下で配布されています。

---

**作者**: mayinchen  
**バージョン**: 2.1  
**最終更新**: 2025年7月

**重要**: このプロジェクトは学習目的で作成されています。本番環境で使用する前に、セキュリティとコスト設定を十分に確認してください。

**v2.1の最適化された機能を活用して、効率的なデータレイク管理を実現してください！**