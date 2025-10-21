# Transform Expression テスト仕様

## 概要
`ApplyTransformExpression` メソッドの変換機能をテストするための仕様書

## 実装対象の変換

### 1. trim(@) - 前後の半角・全角スペース削除

**入力例:**
- `"  Hello  "` → `"Hello"`
- `"　こんにちは　"` (全角スペース) → `"こんにちは"`
- `"  Mixed　Space  "` → `"Mixed　Space"` (内部スペースは保持)

**transform_expr:** `"trim(@)"`

---

### 2. upper(@) - 大文字変換

**入力例:**
- `"hello"` → `"HELLO"`
- `"Hello World"` → `"HELLO WORLD"`
- `"test123"` → `"TEST123"`

**transform_expr:** `"upper(@)"`

---

### 3. nullif(@,'') - 空文字→null

**入力例:**
- `""` → `null`
- `"   "` (空白のみ) → `null`
- `"value"` → `"value"`

**transform_expr:** `"nullif(@,'')"`

---

### 4. to_timestamp(@,'YYYY-MM-DD') - 日付変換

**入力例:**
- `"2025-10-22"` (format: `YYYY-MM-DD`) → `"2025-10-22"`
- `"22/10/2025"` (format: `DD/MM/YYYY`) → `"2025-10-22"`
- `"2025年10月22日"` → パース失敗、元の値を返す

**transform_expr:** `"to_timestamp(@,'YYYY-MM-DD')"`

**対応フォーマット:**
- `YYYY` → 4桁年
- `YY` → 2桁年
- `MM` → 月
- `DD` → 日
- `HH24` → 24時間制の時
- `HH12` → 12時間制の時
- `MI` → 分
- `SS` → 秒
- `MS` → ミリ秒

---

## 複数変換の組み合わせ (パイプライン処理)

**transform_expr:** `"trim(@),upper(@)"`

**処理順:**
1. `trim(@)` を適用
2. `upper(@)` を適用

**入力例:**
- `"  hello  "` → `"  hello  "` → `"hello"` → `"HELLO"`

---

## テストケース

### ケース1: trim のみ
```
入力: "  test  "
transform_expr: "trim(@)"
期待結果: "test"
```

### ケース2: upper のみ
```
入力: "hello"
transform_expr: "upper(@)"
期待結果: "HELLO"
```

### ケース3: trim + upper
```
入力: "  hello  "
transform_expr: "trim(@),upper(@)"
期待結果: "HELLO"
```

### ケース4: nullif
```
入力: ""
transform_expr: "nullif(@,'')"
期待結果: null
```

### ケース5: to_timestamp (YYYY-MM-DD)
```
入力: "2025-10-22"
transform_expr: "to_timestamp(@,'YYYY-MM-DD')"
期待結果: "2025-10-22"
```

### ケース6: to_timestamp (DD/MM/YYYY)
```
入力: "22/10/2025"
transform_expr: "to_timestamp(@,'DD/MM/YYYY')"
期待結果: "2025-10-22"
```

### ケース7: trim + nullif (空文字列)
```
入力: "   "
transform_expr: "trim(@),nullif(@,'')"
期待結果: null
```

### ケース8: 日付パース失敗
```
入力: "invalid-date"
transform_expr: "to_timestamp(@,'YYYY-MM-DD')"
期待結果: "invalid-date" (元の値を返す)
```

---

## エラーハンドリング

1. **null 入力:** `null` を返す
2. **空の transform_expr:** デフォルトの `trim` のみ適用
3. **無効な日付フォーマット:** 元の値を返す
4. **日付パース失敗:** 元の値を返し、ログに警告を出力

---

## 使用例

### CSV インポート設定 (m_data_import_d)

```sql
-- 例1: 商品コードのトリムと大文字化
INSERT INTO m_data_import_d (profile_id, column_seq, projection_kind, attr_cd, target_column, transform_expr, is_required)
VALUES (1, 1, 'PRODUCT', 'PRODUCT_CD', 'product_cd', 'trim(@),upper(@)', TRUE);

-- 例2: 日付フィールドの変換
INSERT INTO m_data_import_d (profile_id, column_seq, projection_kind, attr_cd, target_column, transform_expr, is_required)
VALUES (1, 10, 'PRODUCT_EAV', 'PURCHASE_DATE', NULL, 'to_timestamp(@,''YYYY-MM-DD'')', FALSE);

-- 例3: 空文字をnullに変換
INSERT INTO m_data_import_d (profile_id, column_seq, projection_kind, attr_cd, target_column, transform_expr, is_required)
VALUES (1, 20, 'PRODUCT', 'REMARKS', 'remarks', 'trim(@),nullif(@,'''')', FALSE);
```
