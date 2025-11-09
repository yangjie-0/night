using System;
using System.Text.Json.Serialization;

#nullable enable

namespace ProductDataIngestion.Models
{
    /// <summary>
    /// 映射 category_source_map (カテゴリマッピングテーブル) 表。
    /// 这个类用于将源系统中的多层级分类信息映射到系统统一的全局分类ID (g_category_id)。
    /// </summary>
    public class CategorySourceMap
    {
        // マップID
        [JsonPropertyName("mapId")]
        public long MapId { get; set; }

        // GP会社コード
        [JsonPropertyName("groupCompanyCd")]
        public string GroupCompanyCd { get; set; } = string.Empty;

        // 連携元カテゴリID1階層目
        [JsonPropertyName("sourceCategory1Id")]
        public string? SourceCategory1Id { get; set; }

        // 連携元カテゴリ名1階層目
        [JsonPropertyName("sourceCategory1Nm")]
        public string? SourceCategory1Nm { get; set; }

        // 連携元カテゴリID2階層目
        [JsonPropertyName("sourceCategory2Id")]
        public string? SourceCategory2Id { get; set; }

        // 連携元カテゴリ名2階層目
        [JsonPropertyName("sourceCategory2Nm")]
        public string? SourceCategory2Nm { get; set; }

        // 連携元カテゴリID3階層目
        [JsonPropertyName("sourceCategory3Id")]
        public string? SourceCategory3Id { get; set; }

        // 連携元カテゴリ名3階層目
        [JsonPropertyName("sourceCategory3Nm")]
        public string? SourceCategory3Nm { get; set; }

        // GカテゴリID
        [JsonPropertyName("gCategoryId")]
        public long? GCategoryId { get; set; }

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