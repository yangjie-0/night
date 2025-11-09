using System;
using System.Text.Json.Serialization;

#nullable enable

namespace ProductDataIngestion.Models
{
    /// <summary>
    /// m_category_g (Gカテゴリマスタ) テーブルをマッピングします。
    /// このクラスは、システムにおける階層化されたグローバル統一カテゴリマスターデータを表します。
    /// </summary>
    public class MCategoryG
    {
        // GカテゴリID
        [JsonPropertyName("gCategoryId")]
        public long GCategoryId { get; set; }

        // Gカテゴリコード
        [JsonPropertyName("gCategoryCd")]
        public string GCategoryCd { get; set; } = string.Empty;

        // 親GカテゴリID
        [JsonPropertyName("gCategoryIdParent")]
        public long? GCategoryIdParent { get; set; }

        // Gカテゴリ名
        [JsonPropertyName("gCategoryNm")]
        public string? GCategoryNm { get; set; }

        // 階層レベル
        [JsonPropertyName("hierarchyLevel")]
        public long? HierarchyLevel { get; set; }

        // 表示順
        [JsonPropertyName("gCategorySortNo")]
        public short? GCategorySortNo { get; set; }

        // 有効フラグ
        [JsonPropertyName("isActive")]
        public bool IsActive { get; set; } = true;

        // 登録日時
        [JsonPropertyName("createdAt")]
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

        // 更新日時
        [JsonPropertyName("updatedAt")]
        public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
    }
}