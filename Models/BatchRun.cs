using System;
using System.Text.Json.Serialization;
using System.Text.Json;

namespace ProductDataIngestion.Models
{
    public class BatchRun
    {
        [JsonPropertyName("batchId")]
        public string BatchId { get; set; } = string.Empty;

        [JsonPropertyName("filePath")]
        public string FilePath { get; set; } = string.Empty;

        [JsonPropertyName("groupCompanyCd")]
        public string GroupCompanyCd { get; set; } = string.Empty;

        [JsonPropertyName("targetEntity")]
        public string TargetEntity { get; set; } = string.Empty;

        [JsonPropertyName("totalRecordCount")]
        public int TotalRecordCount { get; set; }

        [JsonPropertyName("successCount")]
        public int SuccessCount { get; set; }

        [JsonPropertyName("errorCount")]
        public int ErrorCount { get; set; }

        [JsonPropertyName("skipCount")]
        public int SkipCount { get; set; }

        [JsonPropertyName("status")]
        public string Status { get; set; } = "PROCESSING";

        [JsonPropertyName("startedAt")]
        public DateTime StartedAt { get; set; } = DateTime.Now;

        // 添加 EndedAt 属性
        [JsonPropertyName("endedAt")]
        public DateTime? EndedAt { get; set; }

        [JsonPropertyName("finishedAt")]
        public DateTime? FinishedAt { get; set; } // 保持向后兼容

        [JsonPropertyName("creAt")]
        public DateTime CreAt { get; set; } = DateTime.Now;

        [JsonPropertyName("updAt")]
        public DateTime UpdAt { get; set; } = DateTime.Now;

        [JsonIgnore]
        public string IdemKey { get; set; } = string.Empty;

        [JsonIgnore]
        public string S3Bucket { get; set; } = "local-development-bucket";

        [JsonIgnore]
        public string Etag { get; set; } = string.Empty;

        [JsonIgnore]
        public string DataKind { get; set; } = "PRODUCT";

        [JsonIgnore]
        public string FileKey { get; set; } = string.Empty;

        [JsonIgnore]
        public string BatchStatus { get; set; } = "RUNNING";

        [JsonIgnore]
        public string CountsJson { get; set; } = "{}";

        public BatchRun()
        {
            InitializeSimulatedData();
        }

        private void InitializeSimulatedData()
        {
            var fileName = System.IO.Path.GetFileName(FilePath);
            var timestamp = DateTime.Now.ToString("yyyyMMddHHmmss");
            
            IdemKey = $"local_{fileName}_{timestamp}";
            Etag = $"local-etag-{Guid.NewGuid():N}";
            DataKind = MapTargetEntityToDataKind(TargetEntity);
            FileKey = $"inbound/{GroupCompanyCd}/{timestamp}/{fileName}";
            BatchStatus = Status;
            UpdateCountsJson();
        }

        private string MapTargetEntityToDataKind(string targetEntity)
        {
            return targetEntity?.ToUpper() switch
            {
                "PRODUCT" => "PRODUCT",
                "EVENT" => "EVENT",
                "STOCK" => "EVENT",
                "PRODUCT_EAV" => "PRODUCT",
                "PRODUCT_MNG" => "PRODUCT_MNG",
                "PRODUCT_IMAGE" => "PRODUCT_IMAGE",
                "PRODUCT_MNG_IMAGE" => "PRODUCT_MNG_IMAGE",
                _ => "PRODUCT"
            };
        }

        public void UpdateCounts(int total, int success, int error, int skip)
        {
            TotalRecordCount = total;
            SuccessCount = success;
            ErrorCount = error;
            SkipCount = skip;
            UpdateCountsJson();
        }

        private void UpdateCountsJson()
        {
            CountsJson = JsonSerializer.Serialize(new
            {
                total = TotalRecordCount,
                success = SuccessCount,
                error = ErrorCount,
                skip = SkipCount,
                ingest = new { read = TotalRecordCount, ok = SuccessCount, ng = ErrorCount },
                cleanse = new { processed = SuccessCount },
                upsert = new { processed = SuccessCount }
            });
        }

        public void Complete()
        {
            Status = SuccessCount > 0 && ErrorCount == 0 ? "SUCCESS" :
                    SuccessCount > 0 && ErrorCount > 0 ? "PARTIAL" : "FAILED";
            BatchStatus = Status;
            EndedAt = DateTime.Now; // 使用 EndedAt
            FinishedAt = DateTime.Now; // 同时更新 FinishedAt 保持兼容
            UpdAt = DateTime.Now;
            UpdateCountsJson();
        }

        public void Fail()
        {
            Status = "FAILED";
            BatchStatus = "FAILED";
            EndedAt = DateTime.Now; // 使用 EndedAt
            FinishedAt = DateTime.Now; // 同时更新 FinishedAt 保持兼容
            UpdAt = DateTime.Now;
            UpdateCountsJson();
        }

        public void Retry()
        {
            Status = "RETRY";
            BatchStatus = "RETRY";
            UpdAt = DateTime.Now;
        }

        public override string ToString()
        {
            return $"BatchRun: {BatchId}, Status: {Status}, Records: {TotalRecordCount} (Success: {SuccessCount}, Error: {ErrorCount}, Skip: {SkipCount})";
        }
    }
}