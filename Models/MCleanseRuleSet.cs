using System;
using System.Text.Json.Serialization;

#nullable enable

namespace ProductDataIngestion.Models
{
    /// <summary>
    /// 映射 m_cleanse_rule_set (クレンジングルールセット) 表。
    /// 各ルールセットのバージョン情報および適用期間を保持する。
    /// </summary>
    public class MCleanseRuleSet
    {
        // ルールセットID
        [JsonPropertyName("ruleSetId")]
        public long RuleSetId { get; set; }

        // ルールバージョン（例: 'v2025.10.01'）
        [JsonPropertyName("ruleVersion")]
        public string RuleVersion { get; set; } = string.Empty;

        // 説明（任意）
        [JsonPropertyName("description")]
        public string? Description { get; set; }

        // ルール適用開始日時
        [JsonPropertyName("releasedAt")]
        public DateTime ReleasedAt { get; set; } = DateTime.UtcNow;

        // 有効フラグ
        [JsonPropertyName("isActive")]
        public bool IsActive { get; set; } = true;

        // 作成者
        [JsonPropertyName("createdBy")]
        public string? CreatedBy { get; set; }

        // 登録日時
        [JsonPropertyName("createdAt")]
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

        // 更新日時
        [JsonPropertyName("updatedAt")]
        public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
    }
}
