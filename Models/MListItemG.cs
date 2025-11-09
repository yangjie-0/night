using System;
using System.Text.Json.Serialization;

#nullable enable

namespace ProductDataIngestion.Models
{
    /// <summary>
    /// 映射 m_list_item_g (Gアイテムリストマスタ) 表。
    /// 这个类代表了系统中全局统一的、用于下拉列表或固定选项的字典主数据。
    /// </summary>
    public class MListItemG
    {
        // GアイテムリストID
        [JsonPropertyName("gListItemId")]
        public long GListItemId { get; set; }

        // リストグループID
        [JsonPropertyName("gListGroupId")]
        public long GListGroupId { get; set; }

        // G項目コード
        [JsonPropertyName("gItemCd")]
        public string GItemCd { get; set; } = string.Empty;

        // G項目表示用ラベル
        [JsonPropertyName("gItemLabel")]
        public string? GItemLabel { get; set; }

        // 別名用
        [JsonPropertyName("synonymsJson")]
        public string SynonymsJson { get; set; } = "{}";

        // 表示順
        [JsonPropertyName("sortOrder")]
        public int? SortOrder { get; set; } = 100;

        // 状態
        [JsonPropertyName("listItemStatus")]
        public string ListItemStatus { get; set; } = "ACTIVE";

        // システム内部利用
        [JsonPropertyName("isSystem")]
        public bool IsSystem { get; set; } = false;

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