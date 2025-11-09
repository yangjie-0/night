# UPSERT処理フロー詳細説明

## 目次
1. [概要](#概要)
2. [全体の流れ](#全体の流れ)
3. [商品データ処理](#商品データ処理)
4. [製品データ処理（KM会社のみ）](#製品データ処理km会社のみ)
5. [重要な注意点](#重要な注意点)

---

## 概要

### UPSERT処理とは
クレンジング済みのデータ（`cl_product_attr`）を、商品マスタテーブル群に書き込む処理です。

### 対象テーブル
1. **m_product_ident** - 商品識別テーブル（商品コードと内部IDの紐付け）
2. **m_product** - 商品マスタ（固定列）
3. **m_product_eav** - 商品EAVマスタ（可変属性）
4. **m_product_management** - 製品マスタ（KM会社のみ）
5. **m_product_management_eav** - 製品EAVマスタ（KM会社のみ）

### トランザクション方針
- **1商品ごとに1トランザクション**
- エラーが発生しても、他の商品の処理は継続
- 長時間のロックを避けるため、バッチ全体を1トランザクションにはしない

---

## 全体の流れ

```
【ステップ0】バッチ開始処理
    ↓
【ステップ1】クレンジング済み属性データ取得（cl_product_attr）
    ↓
【ステップ2】temp_row_id でグループ化（1商品 = 複数の属性）
    ↓
【ステップ3】商品ごとにループ処理
    ├─ 3-1. 必須キーチェック（PRODUCT_CD が必須）
    ├─ 3-2. トランザクション開始
    ├─ 3-3. 商品識別（m_product_ident）
    ├─ 3-4. 商品マスタUPSERT（m_product）
    ├─ 3-5. 商品EAV UPSERT（m_product_eav）
    ├─ 3-6. 【KM会社のみ】製品マスタUPSERT（m_product_management, m_product_management_eav）
    └─ 3-7. トランザクションコミット
    ↓
【ステップ4】バッチ終了処理（統計情報の保存）
```

---

## 商品データ処理

### ステップ3-3: 商品識別（m_product_ident）

#### 目的
連携元の商品コード（`source_product_cd`）と内部ID（`g_product_id`）を紐付ける。

#### 処理の流れ

```
1. 既存の識別レコードを検索
   キー：(group_company_id, source_product_cd, is_active=TRUE)
   ↓
2-A. 【見つからない場合】新規商品
   ・g_product_id を新規採番（例：1, 2, 3...）
   ・m_product_ident に新しい識別レコードを挿入
   ↓
2-B. 【見つかった場合】既存商品
   ・既存の g_product_id を使用
   ・m_product_ident は更新なし
```

#### 書き込む値（新規の場合のみ）

| 列名 | 値 | 説明 |
|-----|----|----|
| g_product_id | 新規採番 | 内部商品ID |
| group_company_id | バッチの会社ID | 会社識別子 |
| source_product_cd | PRODUCT_CD属性の値 | 連携元商品コード |
| source_management_cd | PRODUCT_MANAGEMENT_CD属性の値 | 連携元製品コード（任意） |
| ident_kind | 'AUTO' | 識別方法（自動） |
| confidence | 1.0 | 信頼度 |
| is_primary | TRUE | 主識別フラグ |
| is_active | TRUE | 有効フラグ |
| batch_id | 現在のバッチID | 最終更新バッチ |

---

### ステップ3-4: 商品マスタUPSERT（m_product）

#### 目的
商品の固定列（ブランド、カテゴリ、価格など）を書き込む。

#### 処理の流れ

```
1. 属性データから固定列の値を抽出
   ・is_golden_product = TRUE の属性のみが対象
   ・target_column で指定された列にマッピング
   ↓
2. ブランドID解決（BRAND属性）
   ・BRAND属性の value_cd または value_text から g_brand_cd を取得
   ・m_brand_g テーブルで g_brand_id に変換
   ↓
3. カテゴリID解決（CATEGORY_1属性）
   ・CATEGORY_1属性の value_cd または value_text から g_category_cd を取得
   ・m_category_g テーブルで g_category_id に変換
   ↓
4-A. 【新規商品の場合】INSERT
   ・全ての固定列を設定
   ・初期ステータス（PRODUCT_STATUS_UNKNOWN等）を設定
   ↓
4-B. 【既存商品の場合】UPDATE（差分のみ）
   ・変更があった列のみを更新
   ・差分がなければスキップ
```

#### 固定列の値の取り元

| 列名 | 取得元 | 説明 |
|-----|--------|------|
| g_product_id | m_product_ident | 商品識別テーブルで決定したID |
| g_product_cd | 新規採番 | G商品コード（例：1000000000001） |
| unit_no | 固定値 1 | 枝番（常に1） |
| group_company_id | m_product_ident | 会社ID |
| source_product_cd | m_product_ident | 連携元商品コード |
| source_management_cd | m_product_ident | 連携元製品コード |
| **g_brand_id** | **BRAND属性 → m_brand_g** | **ブランドID** |
| **g_category_id** | **CATEGORY_1属性 → m_category_g** | **カテゴリID（必須）** |
| **currency_cd** | **CURRENCY_CD属性** | **通貨コード** |
| **display_price_incl_tax** | **DISPLAY_PRICE_INCL_TAX属性** | **税込価格** |
| product_status_cd | 'PRODUCT_STATUS_UNKNOWN' | 商品状態（初期値） |
| new_used_kbn_cd | 'PRODUCT_CONDITION_UNKNOWN' | 新品中古区分（初期値） |
| stock_existence_cd | 'STOCK_UNKNOWN' | 在庫有無（初期値） |
| sale_status_cd | 'SALE_UNKNOWN' | 販売可否（初期値） |

#### 重要な注意点
- **g_category_id は必須**：NULL の場合はエラーになります
- **差分更新**：既存商品の場合、値が変わっていない列は更新しません

---

### ステップ3-5: 商品EAV UPSERT（m_product_eav）

#### 目的
商品の可変属性（カテゴリ、ブランド、価格、その他の属性）を書き込む。

#### 処理の流れ

```
1. 既存のEAVデータを全件取得
   ↓
2. 入力属性を1つずつ処理
   ・is_golden_eav = TRUE の属性のみが対象
   ↓
3-A. 【既存データがある場合】差分更新
   ・data_type に応じた列のみをチェック
   ・例：data_type='TEXT' なら value_text のみ
   ・変更があった列のみを更新
   ・差分がなければスキップ
   ↓
3-B. 【既存データがない場合】新規作成
   ・新しいEAVレコードを挿入
   ↓
4. 未出現の属性を非アクティブ化
   ・今回のバッチに含まれなかった属性は is_active = FALSE に設定
   ・再出現したら is_active = TRUE に戻る
```

#### 値の取り元

| 列名 | 取得元 | 説明 |
|-----|--------|------|
| g_product_id | 商品識別で決定 | 内部商品ID |
| attr_cd | cl_product_attr.attr_cd | 属性コード |
| attr_seq | cl_product_attr.attr_seq | 順序番号（なければ1） |
| **value_text** | **data_type='TEXT' の場合のみ** | **文字列値** |
| **value_num** | **data_type='NUM' の場合のみ** | **数値** |
| **value_date** | **data_type='DATE' の場合のみ** | **日付値** |
| **value_cd** | **data_type='LIST' or 'REF' の場合のみ** | **コード値** |
| unit_cd | m_attr_definition.product_unit_cd | 単位コード |
| quality_status | cl_product_attr.quality_status | 品質ステータス（OK/WARN/NG） |
| quality_detail_json | cl_product_attr.quality_detail_json | 品質詳細情報 |
| provenance_json | 自動生成 | 由来情報（下記参照） |
| batch_id | 現在のバッチID | 最終更新バッチ |
| is_active | TRUE | 有効フラグ |

#### provenance_json の内容

```json
{
  "source_system": "KM",                     // 会社コード
  "ingest_profile": "KM_PRODUCT",            // プロファイル
  "idem_key": "batch123:KM:ABC001:BRAND:1",  // 冪等性キー
  "rule_version": "v2025.11.09",             // ルールバージョン
  "dict_hi": ["辞書A", "辞書B"]              // 使用した辞書
}
```

#### data_typeによる値の振り分け

| data_type | 使用する列 | その他の列 |
|-----------|-----------|----------|
| TEXT | value_text | value_num, value_date, value_cd は NULL |
| NUM | value_num | value_text, value_date, value_cd は NULL |
| DATE | value_date | value_text, value_num, value_cd は NULL |
| LIST or REF | value_cd | value_text, value_num, value_date は NULL |

---

## 製品データ処理（KM会社のみ）

### 実行条件
1. **group_company_cd = 'KM'** であること
2. **source_product_management_cd** が存在すること（PRODUCT_MANAGEMENT_CD属性が必要）

### ステップ3-6-1: 製品マスタUPSERT（m_product_management）

#### 目的
商品を製品単位でまとめて管理する。

#### 処理の流れ

```
1. m_product の最新データを再取得
   理由：source_product_management_cd を取得するため
   ↓
2. source_product_management_cd の存在チェック
   なければスキップ
   ↓
3. 既存の製品レコードを検索
   キー：(group_company_id, source_product_management_cd, is_provisional=FALSE)
   ↓
4-A. 【見つからない場合】新規製品の作成
   ・g_product_management_id を新規採番
   ・製品マスタに新しいレコードを挿入
   ↓
4-B. 【見つかった場合】既存製品の更新
   ・既存の g_product_management_id を使用
   ・変更があった列のみを更新
```

#### 値の取り元

| 列名 | 取得元 | 説明 |
|-----|--------|------|
| g_product_management_id | 新規採番 | 内部製品ID |
| group_company_id | m_product | 会社ID |
| source_product_management_cd | m_product | 連携元製品コード（キー） |
| **g_brand_id** | **m_product.g_brand_id** | **ブランドID** |
| **g_category_id** | **m_product.g_category_id** | **カテゴリID（必須）** |
| **description_text** | **CATALOG_DESC属性** | **製品説明** |
| is_provisional | FALSE（固定） | 仮製品フラグ（KMは正式製品） |
| source_product_cd | m_product.g_product_id | 元の商品ID |
| provenance_json | 自動生成 | 由来情報 |
| batch_id | 現在のバッチID | 最終更新バッチ |
| is_active | TRUE | 有効フラグ |

#### 重要なポイント

**なぜ m_product を再取得するのか？**
```
【問題】
ステップ3-4で m_product に source_product_management_cd を書き込んだが、
その前に取得した existingProduct には反映されていない。

【解決】
ステップ3-6の前に m_product を再取得することで、
最新の source_product_management_cd を取得できる。
```

---

### ステップ3-6-2: 製品EAV UPSERT（m_product_management_eav）

#### 目的
製品の可変属性を書き込む。商品EAVとほぼ同じ処理。

#### 商品EAVとの違い

| 項目 | 商品EAV | 製品EAV |
|-----|---------|---------|
| テーブル | m_product_eav | m_product_management_eav |
| 主キー | g_product_id | g_product_management_id |
| 対象条件 | is_golden_eav = TRUE | is_golden_eav = TRUE（同じ） |

#### 処理の流れ

商品EAVとまったく同じです：

```
1. 既存のEAVデータを全件取得
2. 入力属性を1つずつ処理
3-A. 既存データがあれば差分更新
3-B. 既存データがなければ新規作成
4. 未出現の属性を is_active = FALSE にする
```

---

## 重要な注意点

### 1. トランザクション管理

```
✅ 正しい：1商品ごとに1トランザクション
BEGIN TRANSACTION
  ├─ 商品識別
  ├─ 商品マスタUPSERT
  ├─ 商品EAV UPSERT
  └─ 【KM会社】製品マスタUPSERT
COMMIT

❌ 間違い：バッチ全体を1トランザクション
BEGIN TRANSACTION
  ├─ 商品1の処理
  ├─ 商品2の処理
  ├─ ...
  └─ 商品1000の処理
COMMIT（長時間ロック！）
```

### 2. エラー処理

```
商品Aの処理でエラー
   ↓
ROLLBACK（商品Aのトランザクションのみ）
   ↓
record_error テーブルにエラー記録
   ↓
次の商品Bの処理を継続
```

### 3. 必須データのチェック

| データ | 必須度 | なかった場合の動作 |
|-------|-------|------------------|
| PRODUCT_CD | ⚠️ 必須 | スキップ（エラー記録） |
| g_category_id | ⚠️ 必須 | エラーで中断 |
| g_brand_id | ✅ 任意 | NULL で登録 |
| PRODUCT_MANAGEMENT_CD | ✅ 任意 | 製品マスタをスキップ |

### 4. is_golden_product と is_golden_eav の違い

| フラグ | 対象テーブル | 必要なフィールド | 用途 |
|-------|-------------|----------------|------|
| is_golden_product = TRUE | m_product | target_column（必須） | 固定列に書き込む |
| is_golden_eav = TRUE | m_product_eav, m_product_management_eav | target_column（不要） | EAV表に書き込む |

**例：BRAND属性**
```sql
-- BRAND属性の定義
is_golden_product = TRUE   → m_product.g_brand_id に書き込む
is_golden_eav = TRUE       → m_product_eav にも書き込む
target_column = 'g_brand_id'

-- 結果：両方のテーブルに書き込まれる
```

### 5. 差分更新の重要性

```
【なぜ差分更新が必要か？】
・無駄な更新を減らすため
・更新ログを削減するため
・パフォーマンスを向上させるため

【差分更新の仕組み】
既存値：g_brand_id = 100, g_category_id = 200
新値：　g_brand_id = 100, g_category_id = 300

更新するのは g_category_id のみ！
UPDATE m_product SET g_category_id = 300 WHERE g_product_id = 1;
```

### 6. is_active フラグの管理

```
【シナリオ1：新規属性】
バッチ1: BRAND属性が登場 → is_active = TRUE で作成

【シナリオ2：属性が消える】
バッチ2: BRAND属性がない → is_active = FALSE に変更

【シナリオ3：属性が再出現】
バッチ3: BRAND属性が再登場 → is_active = TRUE に戻す
```

---

## まとめ

### 全体の流れ（再掲）

```
クレンジング済みデータ（cl_product_attr）
   ↓
商品ごとにグループ化
   ↓
商品1の処理
 ├─ 商品識別（m_product_ident）
 ├─ 商品マスタ（m_product）
 ├─ 商品EAV（m_product_eav）
 └─ 【KM会社のみ】製品マスタ（m_product_management, m_product_management_eav）
   ↓
商品2の処理
   ↓
...
   ↓
バッチ終了（統計情報保存）
```

### キーポイント

1. ✅ **1商品ごとに1トランザクション**
2. ✅ **エラーが発生しても他の商品の処理は継続**
3. ✅ **差分更新でパフォーマンス向上**
4. ✅ **is_active フラグで属性の出現・消失を管理**
5. ✅ **KM会社のみ製品マスタを作成**

---

## 関連ファイル

- `Services/Upsert/UpsertService.cs` - メインの処理ロジック
- `Repositories/Upsert/UpsertRepository.cs` - 商品・商品EAV用Repository
- `Repositories/Product/ProductManagementRepository.cs` - 製品・製品EAV用Repository
- `Models/Product/MProduct.cs` - 商品マスタのモデル
- `Models/Product/MProductEav.cs` - 商品EAVのモデル
- `Models/Product/MProductManagement.cs` - 製品マスタのモデル
- `Models/Product/MProductManagementEav.cs` - 製品EAVのモデル
