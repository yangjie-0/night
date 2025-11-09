using System;
using System.Text.Json.Serialization;

#nullable enable

namespace ProductDataIngestion.Models
{
    /// <summary>
    /// 映射 attr_source_map (項目マッピングテーブル) 表。
    /// 这个类用于将源系统中的各种属性ID或名称，映射到系统统一的全局列表项ID (g_list_item_id)。
    /// </summary>
    public class AttrSourceMap
    {
        // マップID
        [JsonPropertyName("mapId")]
        public long MapId { get; set; }

        // GP会社コード
        [JsonPropertyName("groupCompanyCd")]
        public string GroupCompanyCd { get; set; } = string.Empty;

        // リストグループID
        [JsonPropertyName("gListGroupId")]
        public long? GListGroupId { get; set; }

        // Gブランドコード
        [JsonPropertyName("gBrandCd")]
        public string? GBrandCd { get; set; }

        // Gカテゴリコード
        [JsonPropertyName("gCategoryCd")]
        public string? GCategoryCd { get; set; }

        // ユースジ
        [JsonPropertyName("usage")]
        public string? Usage { get; set; }

        // 連携元ID
        [JsonPropertyName("sourceAttrId")]
        public string? SourceAttrId { get; set; }

        // 連携元名称
        [JsonPropertyName("sourceAttrNm")]
        public string? SourceAttrNm { get; set; }

        // マッチモード
        [JsonPropertyName("matchMode")]
        public string? MatchMode { get; set; }

        // GアイテムリストID
        [JsonPropertyName("gListItemId")]
        public long? GListItemId { get; set; }

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