namespace ProductDataIngestion.Models
{
    /// <summary>
    /// 属性処理中に検知した警告を保持するシンプルな DTO。
    /// </summary>
    public class WarnInfo
    {
        public string AttrCd { get; set; } = string.Empty;
        public string WarnCode { get; set; } = string.Empty;
        public string Message { get; set; } = string.Empty;
        public string TempRowId { get; set; } = string.Empty;
    }
}
