namespace ProductDataIngestion.Models
{
    public class MFixedToAttrMap
    {
        public long MapId { get; set; }
        public string GroupCompanyCd { get; set; } = string.Empty;
        public string DataKind { get; set; } = string.Empty;
        public string AttrCd { get; set; } = string.Empty;
        public string SourceIdColumn { get; set; } = string.Empty;
        public string SourceLabelColumn { get; set; } = string.Empty;
        public string ValueRole { get; set; } = string.Empty;
        public string DataTypeOverride { get; set; } = string.Empty;
        public string SplitMode { get; set; } = string.Empty;
        public bool IsActive { get; set; }
        public int Priority { get; set; }
        public string FixedRemarks { get; set; } = string.Empty;
    }
}