# ProductDataIngestion システム - アーキテクチャ概要

## 📋 プロジェクト構成

```
ProductDataIngestion/
├── Models/                    # データモデル
│   ├── BatchRun.cs           # バッチ実行情報
│   ├── TempProductParsed.cs  # 一時商品データ
│   ├── ClProductAttr.cs      # 商品属性 (EAV)
│   ├── RecordError.cs        # エラーレコード
│   ├── MDataImportSetting.cs # インポート設定マスタ
│   ├── MDataImportD.cs       # インポート詳細マスタ
│   ├── MCompany.cs           # 会社マスタ
│   └── MFixedToAttrMap.cs    # 固定→属性マッピング
│
├── Repositories/              # データアクセス層 (Repository パターン)
│   ├── IBatchRepository.cs   # バッチリポジトリインターフェース
│   ├── BatchRepository.cs    # バッチリポジトリ実装
│   ├── IProductRepository.cs # 商品リポジトリインターフェース
│   └── ProductRepository.cs  # 商品リポジトリ実装
│
├── Services/                  # ビジネスロジック層
│   ├── IngestService.cs      # CSV取込サービス (メイン)
│   ├── DataImportService.cs  # マスタデータ取得サービス
│   ├── AttributeProcessor.cs # 属性処理専門サービス
│   ├── CsvValidator.cs       # CSV検証ロジック
│   └── IngestException.cs    # 取込処理専用例外 + ErrorCodes定義
│
└── Program.cs                 # エントリーポイント
```

## 🏗️ アーキテクチャパターン

### レイヤー構成

```
┌─────────────────────────────────────────┐
│      Presentation Layer (Program.cs)   │
│         - エントリーポイント             │
│         - 設定読み込み                   │
└─────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────┐
│         Service Layer (Services/)       │
│  - IngestService (メイン処理統括)       │
│  - DataImportService (マスタデータ取得)  │
│  - AttributeProcessor (属性処理専門)    │
│  - CsvValidator (CSV検証ロジック)        │
│  - IngestException (例外 + ErrorCodes)  │
└─────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────┐
│      Repository Layer (Repositories/)   │
│  - IBatchRepository / BatchRepository   │
│  - IProductRepository / ProductRepository│
└─────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────┐
│         Model Layer (Models/)           │
│  - BatchRun, TempProductParsed, etc.    │
│  - RecordError (エラー記録モデル)        │
└─────────────────────────────────────────┘
```

1. **プレゼンテーション層** (`Program.cs`)
   - アプリケーションのエントリーポイント
   - 設定読み込みと依存性注入
   - エラーハンドリング

2. **ビジネスロジック層** (`Services/`)
   - `IngestService`: CSV取込のメインビジネスロジック (10ステップフロー統括)
   - `DataImportService`: マスタデータ取得ロジック (非同期DBアクセス)
   - `AttributeProcessor`: extras_json 解析と属性生成専門
   - `CsvValidator`: CSV データ検証専門クラス (列マッピング、空レコード、必須フィールド検証)
   - `IngestException`: 統一的なエラーハンドリング + `ErrorCodes` 定義

3. **データアクセス層** (`Repositories/`)
   - Repository パターンによるデータベースアクセスの抽象化
   - `BatchRepository`: バッチ実行情報の CRUD
   - `ProductRepository`: 商品データの CRUD

4. **データモデル層** (`Models/`)
   - ドメインモデル定義
   - データベーステーブルとのマッピング
   - `RecordError`: エラー記録モデル (batch_id, step, record_ref, error_cd, error_detail, raw_fragment)

### Repository パターンの採用理由

✅ **関心の分離 (Separation of Concerns)**
   - ビジネスロジックとデータアクセスを分離
   - テスタビリティの向上

✅ **保守性の向上**
   - データベーススキーマ変更時の影響範囲を最小化
   - SQL の一元管理

✅ **再利用性**
   - 他のサービスからも Repository を利用可能

---

## 🔄 ビジネスフロー詳細

### CSV取込処理フロー (10ステップ)

#### **フロー1: バッチ起票**
```
メソッド: CreateBatchRunAsync()
処理内容:
  - batch_id 採番 (BATCH_yyyyMMddHHmmss_GUID)
  - idem_key 生成 (冪等性保証)
  - batch_run テーブルに RUNNING ステータスで登録
  - started_at = now()
```

#### **フロー2: ファイル取込ルールの取得**
```
メソッド: FetchImportRulesAsync()
処理内容:
  - group_company_cd + target_entity で m_data_import_setting を検索
  - is_active = true のチェック
  - profile_id で m_data_import_d を全件取得
エラー処理:
  - ルール不在/重複 → FAILED
```

#### **フロー3: CSV読み込み前のI/O設定**
```
メソッド: ConfigureCsvReaderSettings()
設定項目:
  - character_cd    → 文字コード (UTF-8, Shift_JIS, EUC-JP)
  - delimiter       → 区切り文字 (カンマなど)
  - header_row_index → ヘッダー行位置 (1始まり)
  - skip_row_count  → スキップ行数
```

#### **フロー4: CSV 1行ずつ読込 → 変換**
```
メソッド: ReadAndTransformCsvAsync() → TransformAndMapCsvRow()
処理内容:
  - ヘッダー行まで読み進める (表頭行をスキップ)
  - 列マッピング検証 (column_seq の範囲チェック)
  - 各列に transform_expr 適用 (基本は trim(@))
  - 元CSV値を source_raw として保持
注記:
  - column_seq は 1始まり → 配列インデックスは 0始まりに変換必要
```

#### **フロー5: 必須チェック (is_required)**
```
メソッド: TransformAndMapCsvRow() 内
処理内容:
  - is_required = true の列が空白 → エラー
  - NG行は record_error に記録してスキップ
  - OK行は次の処理へ
```

#### **フロー6: temp への保存**
```
メソッド: SaveToTempTablesAsync() (Repository 経由)
保存先:
  - temp_product_parsed (行テーブル)
  - cl_product_attr (EAV)
  - record_error (エラーレコード)
```

#### **フロー7-9: extras_jsonからデータ取得 → 属性生成 → cl_product_attr保存**
```
メソッド: GenerateProductAttributesAsync()
処理内容:
  1. extras_json から processed_columns を抽出
  2. is_required == true かつ (PRODUCT または PRODUCT_EAV) でフィルタ
  3. 各列について:
     a. attr_cd が空 → エラー (MAPPING_NOT_FOUND)
     b. transformed_value が空 → スキップ
     c. m_fixed_to_attr_map で検索:
        - 存在する場合: ProcessWithFixedMap()
          * value_role == "ID_AND_LABEL": source_id + source_label
          * value_role == "ID_ONLY": source_id のみ
          * value_role == "LABEL_ONLY": source_label のみ
        - 存在しない場合: ProcessWithoutFixedMap()
          * transformed_value を source_id として使用
     d. m_attr_definition から data_type を取得
     e. cl_product_attr レコード作成:
        - batch_id, temp_row_id, attr_cd, attr_seq
        - source_id, source_label, source_raw (JSON)
        - data_type
  4. データベースへ一括保存

AttributeProcessor クラスの主要メソッド:
  - ExtractProcessedColumns(): JSON から列情報を抽出
  - ExtractSourceRaw(): JSON から元データを抽出
  - ProcessWithFixedMap(): マッピング使用時の処理
  - ProcessWithoutFixedMap(): マッピング不使用時の処理
  - FindValueBySourceColumn(): source_column から値を検索
```

#### **フロー10: バッチ統計更新**
```
メソッド: UpdateBatchStatisticsAsync()
処理内容:
  - counts_json 更新 (read/ok/ng カウント)
  - batch_status 更新 (SUCCESS or PARTIAL)
  - ended_at = now()

counts_json 構造例:
{
  "INGEST":  { "read": 1000, "ok": 970, "ng": 30 },
  "CLEANSE": {},
  "UPSERT":  {},
  "CATALOG": {}
}
```

---

## 🔄 Transform Expression (変換式処理)

### 対応する変換関数

#### 1. **trim(@)** - 前後の半角・全角スペース削除
```csharp
入力: "  商品コード  "
出力: "商品コード"
```

#### 2. **upper(@)** - 大文字変換
```csharp
入力: "product_code"
出力: "PRODUCT_CODE"
```

#### 3. **nullif(@,'')** - 空文字を null に変換
```csharp
入力: ""
出力: null

入力: "   "  (空白のみ)
出力: null
```

#### 4. **to_timestamp(@,'YYYY-MM-DD')** - 日付変換
```csharp
入力: "2025-10-22"
フォーマット: 'YYYY-MM-DD'
出力: "2025-10-22" (ISO 8601形式)

対応フォーマット:
- YYYY → yyyy (4桁年)
- YY → yy (2桁年)
- MM → MM (月)
- DD → dd (日)
- HH24 → HH (24時間制)
```

### パイプライン処理 (複数変換の組み合わせ)

```csharp
transform_expr = 'trim(@),upper(@)'

処理順序:
  入力: "  product  "
    ↓ trim(@)
  中間: "product"
    ↓ upper(@)
  出力: "PRODUCT"
```

### データベース設定例

```sql
INSERT INTO m_data_import_d (
    profile_id, column_seq, projection_kind, attr_cd,
    target_column, cast_type, transform_expr, is_required
) VALUES (
    1, 1, 'PRODUCT', 'PRODUCT_CD',
    'product_cd', 'TEXT', 'trim(@),upper(@)', TRUE
);
```

### 実装位置

**ファイル**: `Services/IngestService.cs`

**メソッド**:
- `ApplyTransformExpression(string? value, string transformExpr)` (行662-718)
- `ParseDateExpression(string value, string expression)` (行720-775)
- `ConvertPostgreSqlFormatToDotNet(string pgFormat)` (行777-813)

---

## 📊 主要な改善点

### 1. ✅ extras_json ベースの属性処理への移行
**変更点**:
- データソース: `m_data_import_d` → `temp_product_parsed.extras_json`
- すべての列 (PRODUCT + PRODUCT_EAV) を extras_json に保存
- is_required フィルタは cl_product_attr 挿入時に適用

**影響**:
- データの柔軟性向上 (すべての列情報を保持)
- 処理ロジックの明確化 (保存と検証を分離)

### 2. ✅ Transform Expression 機能の実装
**実装内容**:
- `trim(@)`: 前後スペース削除
- `upper(@)`: 大文字変換
- `nullif(@,'')`: 空文字を null に
- `to_timestamp(@,'YYYY-MM-DD')`: 日付変換

**セキュリティ**:
- ホワイトリスト方式 (定義済み変換のみ実行)
- SQLインジェクション対策 (式は実行されない)

### 3. ✅ AttributeProcessor の分離
**新規クラス**: `Services/AttributeProcessor.cs`

**職責**:
- extras_json からの列情報抽出
- m_fixed_to_attr_map を使用した属性処理
- value_role (ID_AND_LABEL, ID_ONLY, LABEL_ONLY) 対応
- cl_product_attr レコード生成

**利点**:
- 単一責任原則の徹底
- IngestService のコード簡素化
- テスタビリティ向上

### 4. ✅ CsvValidator の導入
**新規クラス**: `Validators/CsvValidator.cs`

**職責**:
- 列マッピング検証
- 空レコード検証
- 必須フィールド検証

**利点**:
- 検証ロジックの一元管理
- IngestService からの責任分離

### 5. ✅ IngestException と ErrorCodes の導入
**新規クラス**: `Exceptions/IngestException.cs`, `Exceptions/ErrorCodes.cs`

**エラーコード**:
- `MISSING_COLUMN`: 列不在
- `EMPTY_RECORD`: 空レコード
- `REQUIRED_FIELD_EMPTY`: 必須フィールド空
- `PARSE_FAILED`: パース失敗
- `MAPPING_NOT_FOUND`: マッピング不在
- `INVALID_ENCODING`: 不正な文字コード
- `DB_ERROR`: データベースエラー

**利点**:
- 統一的なエラーハンドリング
- エラー追跡の容易化

### 6. ✅ JSON プロパティマッピングの修正
**問題**: ProcessedColumnInfo クラスで JSON デシリアライズ失敗
**原因**: snake_case (JSON) と PascalCase (C#) の不一致
**修正**: `[JsonPropertyName]` 属性を追加

```csharp
[System.Text.Json.Serialization.JsonPropertyName("csv_column_index")]
public int CsvColumnIndex { get; set; }
```

### 7. ✅ 未使用メソッドの整理
**コメントアウト**:
- `DataImportService` の同期メソッド (未使用)
- デバッグ用 Console.WriteLine (本番不要)

**影響**:
- コードの可読性向上
- 保守性の向上

---

## 🔧 使用技術

- **言語**: C# (.NET 6+)
- **CSV処理**: CsvHelper
- **データベース**: PostgreSQL
- **ORM**: Dapper
- **設定管理**: Microsoft.Extensions.Configuration

---

## 📝 今後の拡張ポイント

1. **依存性注入 (DI) の導入**
   ```csharp
   // Microsoft.Extensions.DependencyInjection を使用
   services.AddScoped<IBatchRepository, BatchRepository>();
   services.AddScoped<IProductRepository, ProductRepository>();
   services.AddScoped<IngestService>();
   ```

2. **ロギングの強化**
   ```csharp
   // ILogger<T> の導入
   private readonly ILogger<IngestService> _logger;
   ```

3. **ユニットテストの追加**
   ```csharp
   // Repository のモック化によるテスト
   var mockBatchRepo = new Mock<IBatchRepository>();
   var service = new IngestService(connStr, mockBatchRepo.Object, ...);
   ```

4. **エラーハンドリングの詳細化**
   - カスタム例外クラスの定義
   - リトライロジックの実装

---

## 🎯 まとめ

このリファクタリングにより、以下を達成しました:

### アーキテクチャ面
1. ✅ **レイヤー構造の明確化**: 6層アーキテクチャの確立
   - Presentation → Service → Repository → Validation → Exception → Model
2. ✅ **単一責任原則の徹底**: 各クラスが明確な職責を持つ
   - IngestService: フロー統括
   - AttributeProcessor: 属性処理専門
   - CsvValidator: 検証専門
   - DataImportService: マスタデータ取得専門
3. ✅ **関心事の分離**: ビジネスロジック、データアクセス、検証の完全分離

### 機能面
1. ✅ **extras_json ベースの処理**: すべての列情報を保持、柔軟な処理が可能
2. ✅ **Transform Expression**: 4種類の変換関数をパイプライン処理で実行
3. ✅ **PRODUCT + PRODUCT_EAV 対応**: 両方の projection_kind を統一的に処理
4. ✅ **value_role 対応**: ID_AND_LABEL, ID_ONLY, LABEL_ONLY の3パターンを処理

### 品質面
1. ✅ **統一的なエラーハンドリング**: IngestException + ErrorCodes による一貫したエラー処理
2. ✅ **JSON デシリアライズ修正**: JsonPropertyName による正確なマッピング
3. ✅ **未使用コードの整理**: 可読性と保守性の向上
4. ✅ **非同期処理の徹底**: すべての I/O 操作で async/await を使用

### セキュリティ面
1. ✅ **SQLインジェクション対策**: Dapper のパラメータ化クエリ
2. ✅ **Transform Expression の安全性**: ホワイトリスト方式
3. ✅ **エラー情報の適切な制限**: スタックトレースは内部ログのみ

これにより、プロダクション環境で安定稼働可能な、保守性・拡張性・セキュリティを兼ね備えたシステムに到達しました! 🚀

---

**最終更新日**: 2025-10-22
**バージョン**: 2.0
**主要変更**: extras_json ベース処理、AttributeProcessor 分離、Transform Expression 実装、エラーハンドリング強化
