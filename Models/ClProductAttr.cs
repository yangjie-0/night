namespace ProductDataIngestion.Models
{
    public class ClProductAttr
    {
        public string BatchId { get; set; } = string.Empty;
        public Guid TempRowId { get; set; }
        public string AttrCd { get; set; } = string.Empty;
        public short AttrSeq { get; set; }
        public string SourceId { get; set; } = string.Empty;
        public string SourceLabel { get; set; } = string.Empty;
        public string SourceRaw { get; set; } = string.Empty;
        public string ValueText { get; set; } = string.Empty;
        public decimal? ValueNum { get; set; }
        public DateTime? ValueDate { get; set; }
        public string ValueCd { get; set; } = string.Empty;
        public long? GListItemId { get; set; }
        public string DataType { get; set; } = string.Empty;
        public string QualityFlag { get; set; } = string.Empty;
        public string QualityDetailJson { get; set; } = "{}";
        public string ProvenanceJson { get; set; } = "{}";
        public string RuleVersion { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    }
}