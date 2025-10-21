# ProductDataIngestion

CSVファイルから商品データを取り込み、EAV (Entity-Attribute-Value) モデルで属性データを管理するデータインジェストシステム

## 📋 概要

本システムは、複数の会社から提供されるCSVファイルを統一的に取り込み、商品マスタデータを生成するためのデータパイプラインです。

### 主な特徴

- ✅ **柔軟なCSV取込**: 文字コード、区切り文字、ヘッダー行位置などを設定で制御
- ✅ **Transform Expression**: データ変換関数（trim, upper, nullif, to_timestamp）のパイプライン処理
- ✅ **EAVモデル**: 商品属性を柔軟に管理できるEntity-Attribute-Value構造
- ✅ **バッチ管理**: 取込履歴の追跡と統計情報の記録
- ✅ **エラーハンドリング**: レコード単位のエラー記録と継続処理

## 🚀 クイックスタート

### 1. データベースの起動

```bash
cd init
docker-compose up -d
```

### 2. データベースの初期化

```bash
psql -h localhost -p 25432 -U postgres -d product_catalog -f init/initTables.sql
psql -h localhost -p 25432 -U postgres -d product_catalog -f init/insertSettings.sql
```

### 3. 実行

```bash
dotnet run
```

## 📖 ドキュメント

- **[ARCHITECTURE.md](ARCHITECTURE.md)**: アーキテクチャ設計書（レイヤー構造、データフロー、設計原則）
- **[TRANSFORM_EXPRESSION_README.md](Tests/TRANSFORM_EXPRESSION_README.md)**: Transform Expression 機能の詳細説明

## 🏗️ アーキテクチャ

### レイヤー構造

```
Presentation Layer → Service Layer → Repository Layer → Validation Layer → Exception Layer
```

### 処理フロー (10ステップ)

1. バッチ起票
2. ルール取得
3. CSV設定
4-6. CSV読込 → 必須チェック → temp保存
7-9. extras_json解析 → 属性生成 → cl_product_attr保存
10. バッチ統計更新

詳細は [ARCHITECTURE.md](ARCHITECTURE.md) を参照してください。

## 🔧 主要機能

### Transform Expression (変換式処理)

| 関数 | 説明 | 例 |
|------|------|-----|
| `trim(@)` | 前後スペース削除 | `"  ABC  "` → `"ABC"` |
| `upper(@)` | 大文字変換 | `"abc"` → `"ABC"` |
| `nullif(@,'')` | 空文字をnullに | `""` → `null` |
| `to_timestamp(@,'YYYY-MM-DD')` | 日付変換 | `"2025-10-22"` → ISO 8601形式 |

複数の変換をパイプラインで実行可能:

```sql
transform_expr = 'trim(@),upper(@)'  -- 前後スペース削除 → 大文字変換
```

詳細は [TRANSFORM_EXPRESSION_README.md](Tests/TRANSFORM_EXPRESSION_README.md) を参照してください。

## 💾 データベース

### 接続情報

- **Host**: localhost
- **Port**: 25432
- **Database**: product_catalog
- **User**: postgres
- **Password**: postgres

### 主要テーブル

#### マスタテーブル
- `m_company`: 会社マスタ
- `m_data_import_setting`: ファイル取込設定
- `m_data_import_d`: ファイル取込明細 (列マッピング)
- `m_fixed_to_attr_map`: 固定列→属性マッピング
- `m_attr_definition`: 属性定義

#### トランザクションテーブル
- `batch_run`: バッチ実行履歴
- `temp_product_parsed`: 一時商品データ (extras_json に全列情報)
- `cl_product_attr`: 商品属性 (EAV)
- `record_error`: エラーレコード

## 🛠️ 開発環境

### 必須

- **.NET**: 6.0 以上
- **PostgreSQL**: 13.0 以上
- **Docker**: 20.10 以上

### 推奨

- **IDE**: Visual Studio 2022, Visual Studio Code, Rider
- **OS**: Windows 10/11, Linux, macOS
- **RAM**: 4GB 以上

## 📝 使用例

```csharp
// 接続文字列
string connectionString = "Host=localhost;Port=25432;Database=product_catalog;Username=postgres;Password=postgres";

// リポジトリの初期化
var batchRepo = new BatchRepository(connectionString);
var productRepo = new ProductRepository(connectionString);

// IngestServiceの初期化
var ingestService = new IngestService(
    connectionString,
    batchRepo,
    productRepo
);

// CSV取込実行
string batchId = await ingestService.ProcessCsvFileAsync(
    filePath: "path/to/csv/file.csv",
    groupCompanyCd: "KM",
    targetEntity: "PRODUCT"
);

Console.WriteLine($"取込完了: Batch ID = {batchId}");
```

## 🐛 トラブルシューティング

### データベース接続エラー

```
Npgsql.NpgsqlException: Failed to connect to localhost:25432
```

**解決策**:
1. PostgreSQLが起動しているか確認: `docker ps`
2. ポート番号が正しいか確認: `25432`
3. 接続文字列が正しいか確認

### 列マッピングエラー

```
IngestException: 必須列6 (BRAND) がCSV範囲外
```

**解決策**:
1. CSV列数を確認
2. `m_data_import_d.column_seq` が正しいか確認
3. `is_required = true` の列がCSVに存在するか確認

## 📄 ライセンス

本プロジェクトは社内専用です。

---

**最終更新**: 2025-10-22
**バージョン**: 2.0
**メンテナ**: ProductDataIngestion開発チーム
