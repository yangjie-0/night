# Transform Expression 機能説明

## 概要

`transform_expr` は、CSV インポート時にデータを変換するための式を定義する機能です。
`m_data_import_d.transform_expr` カラムに設定された値に基づいて、プログラム内で安全な処理が選択的に適用されます。

---

## 実装済み変換機能

### 1. **trim(@)** - 前後の半角・全角スペース削除

**説明:**
- 文字列の前後にある半角スペース (` `) と全角スペース (`　`) を削除します
- 文字列内部のスペースは保持されます

**使用例:**
```sql
-- m_data_import_d に設定
transform_expr = 'trim(@)'

-- 入力: "  商品コード  "
-- 出力: "商品コード"
```

**C# 実装:**
```csharp
result = value.Trim().Trim('\u3000');
```

---

### 2. **upper(@)** - 大文字変換

**説明:**
- すべての英字を大文字に変換します
- 日本語や数字には影響しません

**使用例:**
```sql
-- m_data_import_d に設定
transform_expr = 'upper(@)'

-- 入力: "product_code"
-- 出力: "PRODUCT_CODE"
```

**C# 実装:**
```csharp
result = value.ToUpper();
```

---

### 3. **nullif(@,'')** - 空文字を null に変換

**説明:**
- 空文字列 (`""`) または空白のみの文字列を `null` に変換します
- データベースに空文字ではなく `NULL` を保存したい場合に使用します

**使用例:**
```sql
-- m_data_import_d に設定
transform_expr = 'nullif(@,'''')'

-- 入力: ""
-- 出力: null

-- 入力: "   "  (空白のみ)
-- 出力: null

-- 入力: "値"
-- 出力: "値"
```

**C# 実装:**
```csharp
if (string.IsNullOrWhiteSpace(result))
{
    result = null;
}
```

---

### 4. **to_timestamp(@,'YYYY-MM-DD')** - 日付変換

**説明:**
- 指定されたフォーマットで日付をパースし、ISO 8601 形式 (`YYYY-MM-DD`) に変換します
- パース失敗時は元の値をそのまま返します (エラーにしない)

**対応フォーマット:**

| PostgreSQL 形式 | .NET 形式 | 説明 |
|----------------|----------|------|
| `YYYY` | `yyyy` | 4桁年 (例: 2025) |
| `YY` | `yy` | 2桁年 (例: 25) |
| `MM` | `MM` | 月 (01-12) |
| `DD` | `dd` | 日 (01-31) |
| `HH24` | `HH` | 24時間制の時 (00-23) |
| `HH12` | `hh` | 12時間制の時 (01-12) |
| `MI` | `mm` | 分 (00-59) |
| `SS` | `ss` | 秒 (00-59) |
| `MS` | `fff` | ミリ秒 |

**使用例:**
```sql
-- 例1: YYYY-MM-DD 形式
transform_expr = 'to_timestamp(@,''YYYY-MM-DD'')'
-- 入力: "2025-10-22"
-- 出力: "2025-10-22"

-- 例2: DD/MM/YYYY 形式
transform_expr = 'to_timestamp(@,''DD/MM/YYYY'')'
-- 入力: "22/10/2025"
-- 出力: "2025-10-22"

-- 例3: YYYY年MM月DD日 形式
transform_expr = 'to_timestamp(@,''YYYY年MM月DD日'')'
-- 入力: "2025年10月22日"
-- 出力: "2025-10-22"

-- 例4: パース失敗の場合
transform_expr = 'to_timestamp(@,''YYYY-MM-DD'')'
-- 入力: "invalid-date"
-- 出力: "invalid-date" (元の値をそのまま返す)
```

**C# 実装:**
```csharp
// PostgreSQL フォーマットを .NET フォーマットに変換
var dotNetFormat = ConvertPostgreSqlFormatToDotNet(formatPattern);

// DateOnly.TryParseExact でパース
if (DateOnly.TryParseExact(value.Trim(), dotNetFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dateOnly))
{
    return dateOnly.ToString("yyyy-MM-dd");
}
else
{
    return value; // パース失敗時は元の値を返す
}
```

---

## 複数変換の組み合わせ (パイプライン処理)

複数の変換を `,` (カンマ) または `;` (セミコロン) で区切って指定できます。
変換は**左から順に**適用されます。

**使用例:**

### 例1: trim + upper
```sql
transform_expr = 'trim(@),upper(@)'

-- 処理順序:
-- 1. trim を適用: "  product  " → "product"
-- 2. upper を適用: "product" → "PRODUCT"

-- 最終結果: "PRODUCT"
```

### 例2: trim + nullif
```sql
transform_expr = 'trim(@),nullif(@,'''')'

-- 処理順序:
-- 1. trim を適用: "   " → ""
-- 2. nullif を適用: "" → null

-- 最終結果: null
```

### 例3: trim + upper + nullif
```sql
transform_expr = 'trim(@),upper(@),nullif(@,'''')'

-- 入力: "  product  "
-- 1. trim: "product"
-- 2. upper: "PRODUCT"
-- 3. nullif: "PRODUCT" (値があるので変換なし)
-- 最終結果: "PRODUCT"

-- 入力: "   "
-- 1. trim: ""
-- 2. upper: ""
-- 3. nullif: null
-- 最終結果: null
```

---

## データベース設定例

### 例1: 商品コードの正規化 (trim + upper)
```sql
INSERT INTO m_data_import_d (
    profile_id, column_seq, projection_kind, attr_cd,
    target_column, cast_type, transform_expr, is_required
) VALUES (
    1, 1, 'PRODUCT', 'PRODUCT_CD',
    'product_cd', 'TEXT', 'trim(@),upper(@)', TRUE
);
```

### 例2: 日付フィールドの変換
```sql
INSERT INTO m_data_import_d (
    profile_id, column_seq, projection_kind, attr_cd,
    target_column, cast_type, transform_expr, is_required
) VALUES (
    1, 10, 'PRODUCT_EAV', 'PURCHASE_DATE',
    NULL, 'TEXT', 'to_timestamp(@,''YYYY-MM-DD'')', FALSE
);
```

### 例3: 空文字を null に変換 (備考欄など)
```sql
INSERT INTO m_data_import_d (
    profile_id, column_seq, projection_kind, attr_cd,
    target_column, cast_type, transform_expr, is_required
) VALUES (
    1, 20, 'PRODUCT', 'REMARKS',
    'remarks', 'TEXT', 'trim(@),nullif(@,'''')', FALSE
);
```

### 例4: ブランド名の正規化 (trim + upper)
```sql
INSERT INTO m_data_import_d (
    profile_id, column_seq, projection_kind, attr_cd,
    target_column, cast_type, transform_expr, is_required
) VALUES (
    1, 6, 'PRODUCT', 'BRAND',
    'brand_nm', 'TEXT', 'trim(@),upper(@)', TRUE
);
```

---

## エラーハンドリング

### 1. null 入力
- **入力:** `null`
- **出力:** `null`
- **動作:** 変換処理をスキップし、そのまま `null` を返します

### 2. 空の transform_expr
- **transform_expr:** `""` または `null`
- **動作:** デフォルトの `trim` のみ適用されます
- **実装:** `value.Trim().Trim('\u3000')`

### 3. 日付パース失敗
- **動作:** 元の値をそのまま返します (例外を投げない)
- **ログ:** 警告メッセージをコンソールに出力
- **例:**
  ```
  [警告] 日付パース失敗: value='invalid-date', format='yyyy-MM-dd'
  ```

### 4. 無効な transform_expr
- **動作:** 認識できない式はスキップされます
- **例:** `transform_expr = 'unknown(@)'` → 何も適用されない

---

## 実装コード位置

**ファイル:** `Services/IngestService.cs`

**メソッド:**
- `ApplyTransformExpression(string? value, string transformExpr)` (行655-710)
- `ParseDateExpression(string value, string expression)` (行716-760)
- `ConvertPostgreSqlFormatToDotNet(string pgFormat)` (行765-793)

---

## テストコード

**ファイル:** `Tests/TransformExpressionExamples.cs`

**実行方法:**
```csharp
TransformExpressionExamples.RunAllExamples();
```

**出力例:**
```
=== Transform Expression サンプル実行 ===

【例1】 trim(@) - 前後のスペース削除
  ✓ 入力: "  Hello  " → 出力: "Hello" (期待: "Hello")
  ✓ 入力: "　こんにちは　" → 出力: "こんにちは" (期待: "こんにちは")
  ✓ 入力: "  Mixed　Space  " → 出力: "Mixed　Space" (期待: "Mixed　Space")

【例2】 upper(@) - 大文字変換
  ✓ 入力: "hello" → 出力: "HELLO" (期待: "HELLO")
  ✓ 入力: "Hello World" → 出力: "HELLO WORLD" (期待: "HELLO WORLD")
  ✓ 入力: "test123" → 出力: "TEST123" (期待: "TEST123")

...
```

---

## セキュリティ上の考慮事項

### SQL インジェクション対策

- ✅ **transform_expr の値は実行されません**
  - データベースに SQL として実行されることはありません
  - プログラム内で事前定義された処理のみが適用されます

### 対応する変換のみ実行

- ✅ **ホワイトリスト方式**
  - `trim(@)`, `upper(@)`, `nullif(@,'')`, `to_timestamp(@,'...')` のみ対応
  - その他の式は無視されます

### 任意コード実行の防止

- ✅ **eval() 等は使用しません**
  - 文字列を動的に評価・実行する処理は一切ありません
  - すべて静的な条件分岐で実装されています

---

## まとめ

| 機能 | 式 | 用途 |
|------|---|------|
| trim | `trim(@)` | 前後のスペース削除 |
| upper | `upper(@)` | 大文字変換 |
| nullif | `nullif(@,'')` | 空文字を null に |
| to_timestamp | `to_timestamp(@,'YYYY-MM-DD')` | 日付変換 |
| 組み合わせ | `trim(@),upper(@)` | パイプライン処理 |

**実装完了日:** 2025-10-22

**関連ファイル:**
- `Services/IngestService.cs` (実装)
- `Tests/TransformExpressionTests.md` (テスト仕様)
- `Tests/TransformExpressionExamples.cs` (サンプルコード)
