using System;
using System.Text.Json.Serialization;

#nullable enable

namespace ProductDataIngestion.Models
{
    /// <summary>
    /// 映射 m_ref_table_map (参照マスタ対応マップ) 表。
    /// 这个类定义了如何为一个参照类型(REF)的属性查找并关联到其对应的主数据表。
    /// </summary>
    public class RefTableMap
    {
        // 参照マップID
        [JsonPropertyName("refMapId")]
        public long RefMapId { get; set; }

        // 項目コード
        [JsonPropertyName("attrCd")]
        public string AttrCd { get; set; } = string.Empty;

        // データソース区分
        [JsonPropertyName("dataSource")]
        public string DataSource { get; set; } = string.Empty;

        // 参照１テーブル
        [JsonPropertyName("hop1Table")]
        public string Hop1Table { get; set; } = string.Empty;

        // 参照１テーブルマッチ方式
        [JsonPropertyName("hop1MatchBy")]
        public string Hop1MatchBy { get; set; } = string.Empty;

        // 参照１テーブル照合ID
        [JsonPropertyName("hop1IdCol")]
        public string? Hop1IdCol { get; set; }

        // 参照１テーブル照合名
        [JsonPropertyName("hop1LabelCol")]
        public string? Hop1LabelCol { get; set; }

        // 参照１テーブル返却値
        [JsonPropertyName("hop1ReturnCols")]
        public string[]? Hop1ReturnCols { get; set; }

        // 参照２テーブル
        [JsonPropertyName("hop2Table")]
        public string? Hop2Table { get; set; }

        // 参照１参照２JOIN
        [JsonPropertyName("hop2JoinOnJson")]
        public string? Hop2JoinOnJson { get; set; }

        // 参照２テーブル最終返却コード列
        [JsonPropertyName("hop2ReturnCdCol")]
        public string? Hop2ReturnCdCol { get; set; }

        // 参照２テーブル最終返却ラベル列
        [JsonPropertyName("hop2ReturnLabelCol")]
        public string? Hop2ReturnLabelCol { get; set; }

        // 備考
        [JsonPropertyName("refTableRemarks")]
        public string? RefTableRemarks { get; set; }

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