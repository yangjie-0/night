using System;
using System.Text.Json.Serialization;

#nullable enable

namespace ProductDataIngestion.Models
{
    /// <summary>
    /// 映射 brand_source_map (ブランドマッピングテーブル) 表。
    /// 这个类用于将不同来源的品牌信息（ID或名称）映射到系统统一的全局品牌ID (g_brand_id)。
    /// </summary>
    public class BrandSourceMap
    {
        // マップID
        [JsonPropertyName("mapId")]
        public long MapId { get; set; }

        // GP会社コード
        [JsonPropertyName("groupCompanyCd")]
        public string GroupCompanyCd { get; set; } = string.Empty;

        // 連携元ブランドID
        [JsonPropertyName("sourceBrandId")]
        public string? SourceBrandId { get; set; }

        // 連携元ブランド名
        [JsonPropertyName("sourceBrandNm")]
        public string? SourceBrandNm { get; set; }

        // 連携元ブランド名(正規化)
        [JsonPropertyName("sourceBrandNmN")]
        public string? SourceBrandNmN { get; set; }

        // GブランドID
        [JsonPropertyName("gBrandId")]
        public long GBrandId { get; set; }

        // 有効フラグ
        [JsonPropertyName("isActive")]
        public bool IsActive { get; set; } = true;

        // 備考
        [JsonPropertyName("sourceMapRemarks")]
        public string? SourceMapRemarks { get; set; }

        // 登録日時
        [JsonPropertyName("createdAt")]
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

        // 更新日時
        [JsonPropertyName("updatedAt")]
        public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
    }
}