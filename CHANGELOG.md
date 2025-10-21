# 変更履歴 (Changelog)

## [2.0.0] - 2025-10-22

### 🎯 主要な変更

#### extras_json ベースの属性処理への移行
- **変更前**: `m_data_import_d` から直接データを取得して `cl_product_attr` に挿入
- **変更後**: `temp_product_parsed.extras_json` にすべての列情報を保存し、そこから取得して処理
- **影響**: データの柔軟性向上、処理ロジックの明確化

#### AttributeProcessor の分離
- **新規クラス**: `Services/AttributeProcessor.cs`
- **職責**: extras_json 解析と属性生成専門
- **メソッド**:
  - `ProcessAttributesAsync()`: 属性処理メイン
  - `ExtractProcessedColumns()`: JSON から列情報を抽出
  - `ExtractSourceRaw()`: JSON から元データを抽出
  - `ProcessWithFixedMap()`: m_fixed_to_attr_map を使用した処理
  - `ProcessWithoutFixedMap()`: m_fixed_to_attr_map を使用しない処理
  - `FindValueBySourceColumn()`: source_column から値を検索

#### Transform Expression 機能の実装
- **新機能**: データ変換関数のパイプライン処理
- **対応関数**:
  1. `trim(@)`: 前後の半角・全角スペース削除
  2. `upper(@)`: 大文字変換
  3. `nullif(@,'')`: 空文字を null に変換
  4. `to_timestamp(@,'YYYY-MM-DD')`: 日付変換
- **セキュリティ**: ホワイトリスト方式、SQL インジェクション対策済み
- **実装位置**: `Services/IngestService.cs`
  - `ApplyTransformExpression()` (行662-718)
  - `ParseDateExpression()` (行720-775)
  - `ConvertPostgreSqlFormatToDotNet()` (行777-813)

#### CsvValidator の導入
- **新規クラス**: `Validators/CsvValidator.cs`
- **職責**: CSV データ検証専門
- **メソッド**:
  - `ValidateColumnMappings()`: 列マッピング検証
  - `ValidateEmptyRecord()`: 空レコード検証
  - `ValidateRequiredFields()`: 必須フィールド検証
- **利点**: 検証ロジックの一元管理、IngestService からの責任分離

#### IngestException と ErrorCodes の導入
- **新規クラス**: `Exceptions/IngestException.cs`
- **新規クラス**: `Exceptions/ErrorCodes.cs`
- **職責**: 統一的なエラーハンドリング
- **エラーコード**:
  - `MISSING_COLUMN`: 列不在
  - `EMPTY_RECORD`: 空レコード
  - `REQUIRED_FIELD_EMPTY`: 必須フィールド空
  - `PARSE_FAILED`: パース失敗
  - `MAPPING_NOT_FOUND`: マッピング不在
  - `INVALID_ENCODING`: 不正な文字コード
  - `DB_ERROR`: データベースエラー
  - その他

### 🐛 バグ修正

#### JSON デシリアライズエラーの修正
- **問題**: `ProcessedColumnInfo` クラスで JSON デシリアライズが失敗
- **原因**: JSON の snake_case と C# の PascalCase の不一致
- **修正**: `[JsonPropertyName]` 属性を全プロパティに追加
- **影響**: PRODUCT_EAV データの正常な挿入が可能に

#### column_seq 0 の正しい処理
- **問題**: `column_seq = 0` の公司コード注入が正しく処理されていない
- **修正**: `column_seq = 0` を公司コード注入として特別処理
- **影響**: 公司コードが正しく extras_json に保存される

#### is_required フィルタの適用タイミング修正
- **変更前**: CSV 読込時に is_required でフィルタ (一部の列が extras_json に保存されない)
- **変更後**: すべての列を extras_json に保存し、cl_product_attr 挿入時にフィルタ
- **影響**: すべての列情報が保持され、柔軟な処理が可能

### 🏗️ アーキテクチャの改善

#### レイヤー構造の明確化
- **6層アーキテクチャの確立**:
  1. Presentation Layer (`Program.cs`)
  2. Service Layer (`Services/`)
  3. Repository Layer (`Repositories/`)
  4. Validation Layer (`Validators/`)
  5. Exception Layer (`Exceptions/`)
  6. Model Layer (`Models/`)

#### 単一責任原則の徹底
- **IngestService**: フロー統括専門
- **AttributeProcessor**: 属性処理専門
- **CsvValidator**: CSV 検証専門
- **DataImportService**: マスタデータ取得専門
- **BatchRepository**: バッチデータ永続化専門
- **ProductRepository**: 商品データ永続化専門

#### 関心事の分離
- ビジネスロジック (Service Layer)
- データアクセス (Repository Layer)
- 検証 (Validation Layer)
- エラーハンドリング (Exception Layer)
の完全分離を実現

### 📝 ドキュメントの追加

#### ARCHITECTURE.md
- **内容**:
  - レイヤー構造図
  - コンポーネント職責
  - データフロー (10ステップ詳細)
  - Transform Expression 説明
  - 設計原則
  - データモデル
  - パフォーマンス考慮事項
  - セキュリティ考慮事項

#### README.md
- **内容**:
  - プロジェクト概要
  - クイックスタート
  - アーキテクチャ概要
  - 主要機能説明
  - データベース情報
  - 使用例
  - トラブルシューティング

#### TRANSFORM_EXPRESSION_README.md
- **内容**:
  - 変換関数の詳細説明
  - パイプライン処理の例
  - データベース設定例
  - エラーハンドリング
  - セキュリティ考慮事項

#### CHANGELOG.md (このファイル)
- **内容**: すべての変更の詳細記録

### 🧹 コード品質の改善

#### 未使用メソッドの整理
- **コメントアウト**: `DataImportService` の同期メソッド (未使用)
- **削除**: デバッグ用 Console.WriteLine (本番不要)
- **影響**: コードの可読性向上、保守性向上

#### 日本語コメントの追加
- すべての主要クラス、メソッドに簡潔な日本語コメントを追加
- XML ドキュメントコメント (`/// <summary>`) の統一的な使用

#### リージョンの活用
- `IngestService.cs` でリージョンを使用した論理的なグループ化
- 各フローステップを明確に区分

### ⚙️ データベーススキーマの修正

#### m_attr_definition テーブル
- **問題**: 列名の不一致 (`is_golden_attr_PRODUCT_EAV` vs `is_golden_attr_eav`)
- **修正**: INSERT 文の列名を `is_golden_attr_eav` に統一
- **影響**: マスタデータの正常な投入が可能

### 🔒 セキュリティの強化

#### SQL インジェクション対策
- Dapper のパラメータ化クエリを使用
- すべての SQL 文でパラメータバインディングを実施

#### Transform Expression の安全性
- ホワイトリスト方式で定義済み変換のみ実行
- eval() や動的コード実行を一切使用しない
- すべて静的な条件分岐で実装

#### エラー情報の適切な制限
- スタックトレースは内部ログのみに記録
- ユーザーには必要最小限のエラー情報を提供

### 🚀 パフォーマンスの最適化

#### ストリーミング処理
- CSV を1行ずつ読み込み、メモリ効率を向上
- 大容量ファイルでもメモリ不足にならない設計

#### バッチ保存
- `temp_product_parsed` と `cl_product_attr` をまとめて保存
- データベースアクセス回数を最小化

#### 非同期I/O
- すべてのデータベースアクセスを非同期化
- `async/await` パターンの徹底

#### JSON 最適化
- `System.Text.Json` を使用した高速な JSON 処理
- 不要な JSON シリアライズ/デシリアライズを削減

## [1.0.0] - 2025-10-14

### 🎉 初版リリース

#### 基本機能の実装
- CSV ファイル取込処理
- バッチ管理機能
- エラーハンドリング
- Repository パターンの採用

---

## 変更タイプの凡例

- 🎯 **主要な変更**: システムに大きな影響を与える変更
- 🐛 **バグ修正**: 不具合の修正
- 🏗️ **アーキテクチャ**: 設計や構造の改善
- 📝 **ドキュメント**: ドキュメントの追加・更新
- 🧹 **コード品質**: リファクタリング、コメント追加など
- ⚙️ **設定**: 設定ファイルやスキーマの変更
- 🔒 **セキュリティ**: セキュリティ関連の改善
- 🚀 **パフォーマンス**: パフォーマンス最適化

---

**最終更新**: 2025-10-22
**バージョン**: 2.0.0
