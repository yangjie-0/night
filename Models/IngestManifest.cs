namespace ProductDataIngestion.Models
{
    public class IngestManifest
    {
        public string BatchId { get; set; } = string.Empty;
        public string S3Bucket { get; set; } = string.Empty;
        public string ObjectKey { get; set; } = string.Empty;
        public string ETag { get; set; } = string.Empty;
        public int RowCount { get; set; }
        public string MetaJson { get; set; } = "{}";
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
        public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
    }
}