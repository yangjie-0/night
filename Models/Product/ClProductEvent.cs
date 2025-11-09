using System;

namespace ProductDataIngestion.Models
{
    /// <summary>
    /// cl_product_event テーブルを表すモデル。
    /// イベント系 UPSERT に利用する。
    /// </summary>
    public class ClProductEvent
    {
        public long IdEventId { get; set; }
        public Guid TempRowEventId { get; set; }
        public string BatchId { get; set; } = string.Empty;
        public string IdemKey { get; set; } = string.Empty;
        public string? StockEffectCd { get; set; }
        public int? SignedQtyNum { get; set; }
        public string? ReversalIdemKey { get; set; }
        public long GroupCompanyId { get; set; }
        public long GProductId { get; set; }
        public long StoreId { get; set; }
        public string? NewUsedKbnCd { get; set; }
        public DateTime EventTs { get; set; }
        public string EventKindCd { get; set; } = string.Empty;
        public int QtyNum { get; set; }
        public string? QualityStatus { get; set; }
        public string? QualityDetailJson { get; set; }
        public string? ProvenanceJson { get; set; }
        public string? RuleVersion { get; set; }
        public string CleanseStatus { get; set; } = "READY";
        public string? UpsertStatus { get; set; }
        public DateTime CreAt { get; set; }
        public DateTime UpdAt { get; set; }
        public string? StockEffectSummary => StockEffectCd;
    }
}
