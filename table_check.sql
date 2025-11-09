-- ============================================
-- テーブル構造確認スクリプト
-- m_product_management と m_product_management_eav の
-- 存在と列構造を確認します
-- ============================================

-- ステップ1：接続情報の確認
SELECT
    current_database() AS "接続中のDB",
    current_user AS "接続ユーザー";

-- ステップ2：製品マスタ関連テーブルの存在確認
SELECT
    table_name AS "テーブル名",
    CASE
        WHEN table_type = 'BASE TABLE' THEN '✅ 存在する'
        ELSE '⚠️ 存在しない'
    END AS "状態"
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN ('m_product_management', 'm_product_management_eav')
ORDER BY table_name;

-- ステップ3：m_product_management の列構造を確認
-- 注意：batch_id 列が position 10 に存在するか確認してください
SELECT
    ordinal_position AS "位置",
    column_name AS "列名",
    data_type AS "データ型",
    CASE
        WHEN is_nullable = 'YES' THEN 'YES'
        ELSE 'NO'
    END AS "NULL許可",
    column_default AS "デフォルト値"
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'm_product_management'
ORDER BY ordinal_position;

-- ステップ4：m_product_management_eav の列構造を確認
-- 注意：batch_id 列が position 10 に存在するか確認してください
SELECT
    ordinal_position AS "位置",
    column_name AS "列名",
    data_type AS "データ型",
    CASE
        WHEN is_nullable = 'YES' THEN 'YES'
        ELSE 'NO'
    END AS "NULL許可",
    column_default AS "デフォルト値"
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'm_product_management_eav'
ORDER BY ordinal_position;

-- ステップ5：batch_id 列の存在を直接確認
SELECT
    table_name AS "テーブル名",
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'm_product_management'
              AND column_name = 'batch_id'
        ) THEN '✅ batch_id 列が存在する'
        ELSE '❌ batch_id 列が存在しない'
    END AS "m_product_management の batch_id",
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'm_product_management_eav'
              AND column_name = 'batch_id'
        ) THEN '✅ batch_id 列が存在する'
        ELSE '❌ batch_id 列が存在しない'
    END AS "m_product_management_eav の batch_id";

-- ステップ6：すべてのスキーマとテーブルを確認（念のため）
-- 他のスキーマに同名のテーブルがないか確認
SELECT
    table_schema AS "スキーマ",
    table_name AS "テーブル名",
    table_type AS "タイプ"
FROM information_schema.tables
WHERE table_name IN ('m_product_management', 'm_product_management_eav')
ORDER BY table_schema, table_name;

-- ============================================
-- 診断結果の解釈
-- ============================================
--
-- ステップ2で「存在しない」の場合：
--   → テーブルがまだ作成されていません
--   → init/createTable*.sql を実行してください
--
-- ステップ5で「batch_id 列が存在しない」の場合：
--   → テーブルは存在するが、古いスキーマで作成されています
--   → ALTER TABLE で batch_id 列を追加するか、
--     テーブルを削除して再作成してください
--
-- ステップ3/4で列の位置が異なる場合：
--   → スキーマ定義とデータベースの実際の構造が一致していません
--   → テーブルを削除して最新のCREATE TABLEスクリプトで再作成してください
--
-- ステップ6で複数のスキーマに同名テーブルがある場合：
--   → 接続しているスキーマ(public)が正しいか確認してください
-- ============================================
