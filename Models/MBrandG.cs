using System;
using System.Text.Json.Serialization;

#nullable enable

namespace ProductDataIngestion.Models
{
    /// <summary>
    /// 映射 m_brand_g (Gブランドマスタ) 表。
    /// 这个类代表了系统中的全局统一品牌主数据。
    /// </summary>
    public class MBrandG
    {
        // GブランドID
        [JsonPropertyName("gBrandId")]
        public long GBrandId { get; set; }

        // Gブランドコード
        [JsonPropertyName("gBrandCd")]
        public string GBrandCd { get; set; } = string.Empty;

        // Gブランド名
        [JsonPropertyName("gBrandNm")]
        public string? GBrandNm { get; set; }

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