-- 迁移脚本: 将 m_fixed_to_attr_map 表的 data_kind 列改为 projection_kind
-- 执行日期: 2025-10-26
-- 说明: batch_run 表保持使用 data_kind，只修改 m_fixed_to_attr_map 表

-- 检查列是否存在，如果是 data_kind 则重命名为 projection_kind
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'm_fixed_to_attr_map'
        AND column_name = 'data_kind'
    ) THEN
        ALTER TABLE m_fixed_to_attr_map
        RENAME COLUMN data_kind TO projection_kind;

        RAISE NOTICE 'm_fixed_to_attr_map.data_kind 已重命名为 projection_kind';
    ELSE
        RAISE NOTICE 'm_fixed_to_attr_map 表中已经是 projection_kind 列，无需迁移';
    END IF;
END $$;
