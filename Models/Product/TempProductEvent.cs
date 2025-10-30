namespace ProductDataIngestion.Models
{
    /// <summary>
    /// 一時的に解析したイベント（在庫/販売など）データを保持するモデル（行単位）
    /// temp_product_event テーブルに対応。
    /// 主要固定列は TEXT のまま保存し、元値は source_raw として保持します。
    /// </summary>
    public class TempProductEvent
    {
        public Guid TempRowEventId { get; set; }
        public string BatchId { get; set; } = string.Empty;
        public long TimeNo { get; set; }  // DB列名: time_no (CSV行番号)
        public string IdemKey { get; set; } = string.Empty;  // 冪等キー: {batch_id}:{time_no} (UNIQUE)
        public string SourceGroupCompanyCd { get; set; } = string.Empty;
        public string? SourceProductId { get; set; }  // DB列名: source_product_id

        // 固定列（すべて TEXT のまま）
        public string? SourceStoreIdRaw { get; set; }
        public string? SourceStoreNmRaw { get; set; }
        public string? SourceNewUsedKbnRaw { get; set; }
        public string? QtyRaw { get; set; }
        public string? EventTsRaw { get; set; }
        public string? EventKindRaw { get; set; }

        public string StepStatus { get; set; } = "READY";
        public string ExtrasJson { get; set; } = "{}";
    }
}

