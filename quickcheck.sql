-- ============================================
-- 快速診断スクリプト（安全版）
-- pgAdmin4 のクエリエディタで実行してください
-- ============================================

-- ステップ0：トランザクション状態の確認
SELECT
    CASE
        WHEN pg_backend_pid() IN (SELECT pid FROM pg_stat_activity WHERE state = 'idle in transaction')
        THEN '⚠️ トランザクション中（未コミット）'
        ELSE '✅ トランザクション外（正常）'
    END AS "トランザクション状態";

-- ステップ1：接続情報の確認
SELECT
    current_database() AS "接続中のDB",
    current_user AS "接続ユーザー",
    version() AS "PostgreSQLバージョン";

-- ステップ2：テーブルの存在確認
SELECT
    table_name AS "テーブル名",
    CASE
        WHEN table_type = 'BASE TABLE' THEN '✅ 存在する'
        ELSE '❌ 存在しない'
    END AS "状態"
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN (
    'm_product',
    'm_product_eav',
    'm_product_ident',
    'cl_product_attr',
    'm_product_management',
    'm_product_management_eav',
    'batch_run',
    'record_error',
    'm_attr_definition'
  )
ORDER BY table_name;

-- ステップ3：各テーブルのデータ件数を確認（存在するテーブルのみ）
-- 3-1: m_product
SELECT 'm_product' AS "テーブル名", COUNT(*) AS "件数"
FROM m_product;

-- 3-2: m_product_eav
SELECT 'm_product_eav' AS "テーブル名", COUNT(*) AS "件数"
FROM m_product_eav;

-- 3-3: m_product_ident
SELECT 'm_product_ident' AS "テーブル名", COUNT(*) AS "件数"
FROM m_product_ident;

-- 3-4: cl_product_attr
SELECT 'cl_product_attr' AS "テーブル名", COUNT(*) AS "件数"
FROM cl_product_attr;

-- ステップ4：m_product_management テーブルの存在確認と列構造確認
-- まずテーブルが存在するか確認
SELECT
    table_name AS "テーブル名",
    CASE
        WHEN table_type = 'BASE TABLE' THEN '✅ 存在する'
        ELSE '⚠️ 存在しない'
    END AS "状態"
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN ('m_product_management', 'm_product_management_eav');

-- m_product_management の実際の列構造を確認
SELECT
    ordinal_position AS "位置",
    column_name AS "列名",
    data_type AS "データ型",
    is_nullable AS "NULL許可",
    column_default AS "デフォルト値"
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'm_product_management'
ORDER BY ordinal_position;

-- m_product_management_eav の実際の列構造を確認
SELECT
    ordinal_position AS "位置",
    column_name AS "列名",
    data_type AS "データ型",
    is_nullable AS "NULL許可"
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'm_product_management_eav'
ORDER BY ordinal_position;

-- ステップ8：最新のバッチ処理状態
SELECT
    batch_id,
    batch_status,
    group_company_cd,
    data_kind,
    started_at,
    ended_at,
    LEFT(counts_json::text, 200) AS "統計情報（抜粋）"
FROM batch_run
ORDER BY started_at DESC
LIMIT 3;

-- ステップ5：最新の m_product レコードを確認
SELECT
    g_product_id,
    g_product_cd,
    source_product_cd,
    source_product_management_cd,
    g_brand_id,
    g_category_id,
    cre_at
FROM m_product
ORDER BY cre_at DESC
LIMIT 5;

-- ステップ6：最新の m_product_eav レコードを確認
SELECT
    g_product_id,
    attr_cd,
    attr_seq,
    COALESCE(value_text, value_num::text, value_cd) AS "値",
    cre_at
FROM m_product_eav
ORDER BY cre_at DESC
LIMIT 10;

-- ステップ7：エラーが記録されていないか確認
SELECT
    temp_row_id,
    step,
    error_cd,
    LEFT(error_message, 100) AS "エラーメッセージ（抜粋）",
    occurred_at
FROM record_error
ORDER BY occurred_at DESC
LIMIT 10;

-- ステップ7：属性定義を確認（is_golden_eav の設定）
SELECT
    attr_cd,
    is_golden_product,
    is_golden_eav AS "EAV対象",
    target_column,
    data_type,
    is_active
FROM m_attr_definition
WHERE is_active = TRUE
ORDER BY attr_cd
LIMIT 20;

-- ステップ9：製品マスタのデータ件数確認（テーブルが存在する場合のみ実行）
-- 注意：上記のステップ4で m_product_management が存在することを確認してから実行してください
-- もしテーブルが存在しない場合、またはbatch_id列がない場合は、このクエリはスキップしてください

-- 製品マスタのデータ件数（テーブルが存在し、batch_id列がある場合のみ）
-- SELECT 'm_product_management' AS "テーブル名", COUNT(*) AS "件数"
-- FROM m_product_management;

-- 製品マスタEAVのデータ件数（テーブルが存在し、batch_id列がある場合のみ）
-- SELECT 'm_product_management_eav' AS "テーブル名", COUNT(*) AS "件数"
-- FROM m_product_management_eav;
