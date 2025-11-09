// 文件夹: Models
// 文件名: CleansePolicy.cs

using System;
using System.Text.Json.Serialization;

#nullable enable

namespace ProductDataIngestion.Models
{
    /// <summary>
    /// 映射 m_attr_cleanse_policy (ルール定義テーブル) 表。
    /// 这个类定义了针对每个数据属性的具体清洗规则和执行策略。
    /// </summary>
    public class CleansePolicy
    {
        // ポリシーID
        [JsonPropertyName("policyId")]
        public long PolicyId { get; set; }

        // ルールセットID
        [JsonPropertyName("ruleSetId")]
        public long RuleSetId { get; set; }

        // 項目コード
        [JsonPropertyName("attrCd")]
        public string AttrCd { get; set; } = string.Empty;

        // データタイプ
        [JsonPropertyName("dataType")]
        public string DataType { get; set; } = string.Empty;

        // 参照マップID
        [JsonPropertyName("refMapId")]
        public long? RefMapId { get; set; }

        // リストグループコード
        [JsonPropertyName("gListGroupCd")]
        public string? GListGroupCd { get; set; }

        // 限定対象GP会社
        [JsonPropertyName("gpScope")]
        public string? GpScope { get; set; }

        // 限定対象カテゴリ
        [JsonPropertyName("categoryScope")]
        public string? CategoryScope { get; set; }

        // 限定対象ブランド
        [JsonPropertyName("brandScope")]
        public string? BrandScope { get; set; }

        // 優先順位
        [JsonPropertyName("stepNo")]
        public short StepNo { get; set; }

        // 照合方式
        [JsonPropertyName("matcherKind")]
        public string MatcherKind { get; set; } = string.Empty;

        // 派生優先順リスト
        [JsonPropertyName("deriveFromAttrCds")]
        public string[]? DeriveFromAttrCds { get; set; }

        // 値分割記号
        [JsonPropertyName("splitMode")]
        public string? SplitMode { get; set; }

        // 打ち切り
        [JsonPropertyName("stopOnHit")]
        public bool StopOnHit { get; set; } = true;

        // 類似度
        [JsonPropertyName("threshold")]
        public decimal? Threshold { get; set; }

        // 有効フラグ
        [JsonPropertyName("isActive")]
        public bool IsActive { get; set; } = true;

        // 備考
        [JsonPropertyName("importRemarks")]
        public string? ImportRemarks { get; set; }

        // 作成者
        [JsonPropertyName("creBy")]
        public string? CreBy { get; set; }

        // 更新者
        [JsonPropertyName("updBy")]
        public string? UpdBy { get; set; }

        // 登録日時
        [JsonPropertyName("createdAt")]
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

        // 更新日時
        [JsonPropertyName("updatedAt")]
        public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
    }
}