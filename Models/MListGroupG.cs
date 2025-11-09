using System;
using System.Text.Json.Serialization;

#nullable enable

namespace ProductDataIngestion.Models
{
    /// <summary>
    /// m_list_group_g (Gアイテムリストグループマスタ) テーブルをマッピングします。
    /// このクラスは、m_list_item_g の各項目をグループ化するために使用されます。
    /// </summary>
    public class MListGroupG
    {
        // リストグループID
        [JsonPropertyName("gListGroupId")]
        public long GListGroupId { get; set; }

        // リストグループコード
        [JsonPropertyName("gListGroupCd")]
        public string GListGroupCd { get; set; } = string.Empty;

        // リストグループ名
        [JsonPropertyName("gListGroupNm")]
        public string GListGroupNm { get; set; } = string.Empty;

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