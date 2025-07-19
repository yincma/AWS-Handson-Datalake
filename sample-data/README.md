# サンプルデータセット

本ディレクトリには、AWSデータレイク実験用のサンプルEコマースデータが含まれています。データは多言語Eコマースプラットフォームのビジネスシナリオをシミュレートし、日本語と英語の顧客データを含んでいます。

## データファイル概要

### 1. customers.csv
顧客基本情報テーブル、15レコードを含む。

**フィールド説明：**
- `customer_id`: 顧客一意識別子
- `first_name/last_name`: 顧客氏名
- `email`: メールアドレス
- `phone`: 電話番号（一部欠損、データクリーニングテスト用）
- `address/city/state/zip_code/country`: 住所情報
- `registration_date`: 登録日
- `last_login`: 最終ログイン時間（一部欠損）
- `customer_segment`: 顧客レベル（VIP/Premium/Standard）
- `sensitive_ssn`: 機密情報（Lake Formation列レベル権限テスト用）

**データ特徴：**
- 日米両国の顧客データを含む
- 意図的に欠損値（phone、last_login）を含みデータクリーニングをテスト
- 権限制御テスト用の機密フィールドを含む

### 2. orders.csv
注文メインテーブル、20レコードを含む。

**フィールド説明：**
- `order_id`: 注文一意識別子
- `customer_id`: 関連顧客ID
- `order_date`: 注文日（データ標準化テスト用の異なる形式を含む）
- `order_status`: 注文状態（completed/shipped/processing/cancelled）
- `payment_method`: 支払い方法
- `shipping_address/billing_address`: 配送先住所と請求先住所
- `total_amount/discount_amount/tax_amount/shipping_cost`: 金額情報
- `currency`: 通貨タイプ（JPY/USD）

**データ特徴：**
- 複数の日付形式を含む（2024-01-01 10:30:00、01/09/2024 12:15、Jan 14 2024 16:30）
- 日米両通貨をサポート
- 分析用の異なる注文状態を含む

### 3. products.csv
製品情報テーブル、20レコードを含む。

**フィールド説明：**
- `product_id`: 製品一意識別子
- `product_name`: 製品名
- `category/subcategory`: 製品分類
- `brand`: ブランド
- `price/cost`: 価格とコスト
- `weight_kg/dimensions_cm`: 物理属性
- `color/size/material`: 外観属性
- `description`: 製品説明
- `stock_quantity/min_stock_level`: 在庫情報
- `supplier_id`: サプライヤーID
- `launch_date`: 発売日

**データ特徴：**
- 電子製品、ファッション、ホーム、ヘルスなど複数カテゴリーをカバー
- 多次元分析用の完全な製品属性を含む
- 価格範囲は89円から8999円

### 4. order_items.csv
注文アイテム詳細テーブル、30レコードを含む。

**フィールド説明：**
- `order_item_id`: 注文アイテム一意識別子
- `order_id`: 関連注文ID
- `product_id`: 関連製品ID
- `quantity`: 数量
- `unit_price/total_price`: 単価と総価格
- `discount_applied`: 適用された割引
- `item_status`: アイテム状態

## データ関係

```
customers (1) ←→ (N) orders (1) ←→ (N) order_items (N) ←→ (1) products
```

## 使用シナリオ

### データクリーニングテスト
- 日付形式標準化（orders.order_date）
- 欠損値処理（customers.phone、customers.last_login）
- データ型変換

### 権限制御テスト
- 列レベル権限：`customers.sensitive_ssn`フィールド
- テーブルレベル権限：異なるロールによる異なるテーブルアクセス
- 行レベル権限：国や顧客レベル別フィルタリング

### 分析シナリオ
- 顧客価値分析（customer_segment別グループ化）
- 製品売上分析（最人気製品/カテゴリー）
- 注文トレンド分析（時間軸別）
- 国際Eコマース分析（JPY vs USD注文）
- 在庫管理分析

## S3アップロードの推奨方法

```bash
# Raw層へのアップロード
aws s3 cp customers.csv s3://your-project-raw/ecommerce/customers/
aws s3 cp orders.csv s3://your-project-raw/ecommerce/orders/
aws s3 cp products.csv s3://your-project-raw/ecommerce/products/
aws s3 cp order_items.csv s3://your-project-raw/ecommerce/order_items/
```

## データ品質問題（意図的設計）

1. **日付形式の不整合**: orders.csvのorder_dateフィールド
2. **欠損値**: customersテーブルのphoneとlast_loginフィールド
3. **データ型混合**: 異なる通貨の金額フィールド
4. **文字エンコーディング**: 国際化サポートテスト用の日本語文字を含む

これらの問題は意図的に設計されており、データクリーニングと標準化プロセスのデモンストレーション用です。