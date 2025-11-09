using System;

namespace ProductDataIngestion.Models
{
    /// <summary>
    /// m_product_eav テーブルの1レコードを表すモデル。
    /// UPSERT 処理で差分検出と値更新に使用する。
    /// </summary>
    // シンプル説明: 任意属性(EAV: 可変項目)を保持するテーブルの行。
    public class MProductEav
    {
        public long GProductId { get; set; }
        public string AttrCd { get; set; } = string.Empty;
        public short AttrSeq { get; set; } = 1;
        public string? ValueText { get; set; }
        public decimal? ValueNum { get; set; }
        public DateTime? ValueDate { get; set; }
        public string? ValueCd { get; set; }
        public string? UnitCd { get; set; }
        public string? QualityStatus { get; set; }
        public string? QualityDetailJson { get; set; }
        public string? ProvenanceJson { get; set; }
        public string? BatchId { get; set; }
        public bool IsActive { get; set; } = true;
        public DateTime CreAt { get; set; }
        public DateTime UpdAt { get; set; }
    }
}
