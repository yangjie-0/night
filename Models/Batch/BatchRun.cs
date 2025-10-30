using System;
using System.Text.Json.Serialization;
using System.Text.Json;

namespace ProductDataIngestion.Models
{
    /// <summary>
    /// バッチ実行のメタ情報を表すモデル。
    /// バッチID、ファイル情報、件数、状態など実行単位のメタデータを保持する。
    /// 主にログ記録やステータス表示、デバッグ用に使われる。
    /// </summary>
    public class BatchRun
    {
        /// <summary>
        /// バッチ実行を一意に識別するID。
        /// </summary>
        [JsonPropertyName("batchId")]
        public string BatchId { get; set; } = string.Empty;

    /// <summary>
    /// 処理対象ファイルのパス（ローカルまたはS3のキーなど）。
    /// </summary>
    [JsonPropertyName("filePath")]
    public string FilePath { get; set; } = string.Empty;

    /// <summary>
    /// 所属企業コード（グループ会社コード）。
    /// </summary>
    [JsonPropertyName("groupCompanyCd")]
    public string GroupCompanyCd { get; set; } = string.Empty;

    /// <summary>
    /// 対象エンティティ名（例: PRODUCT, EVENT）。処理区分に使われる。
    /// </summary>
    [JsonPropertyName("targetEntity")]
    public string TargetEntity { get; set; } = string.Empty;

    /// <summary>
    /// 読み込んだレコードの総数。
    /// </summary>
    [JsonPropertyName("totalRecordCount")]
    public int TotalRecordCount { get; set; }

    /// <summary>
    /// 正常に処理されたレコード数。
    /// </summary>
    [JsonPropertyName("successCount")]
    public int SuccessCount { get; set; }

    /// <summary>
    /// エラーとなったレコード数。
    /// </summary>
    [JsonPropertyName("errorCount")]
    public int ErrorCount { get; set; }

    /// <summary>
    /// スキップされたレコード数（処理対象外等）。
    /// </summary>
    [JsonPropertyName("skipCount")]
    public int SkipCount { get; set; }

    /// <summary>
    /// バッチのステータス（PROCESSING, SUCCESS, FAILED 等）。
    /// </summary>
    [JsonPropertyName("status")]
    public string Status { get; set; } = "PROCESSING";

    /// <summary>
    /// バッチ開始日時。
    /// </summary>
    [JsonPropertyName("startedAt")]
    public DateTime StartedAt { get; set; } = DateTime.Now;

    // EndedAt 属性
    /// <summary>
    /// バッチ終了日時（正常終了/異常終了いずれでも設定）。
    /// </summary>
    [JsonPropertyName("endedAt")]
    public DateTime? EndedAt { get; set; }

    /// <summary>
    /// 完了日時（互換性保持用）。
    /// </summary>
    [JsonPropertyName("finishedAt")]
    public DateTime? FinishedAt { get; set; } // 保持向后兼容

    /// <summary>
    /// レコード作成日時（メタ情報）。
    /// </summary>
    [JsonPropertyName("creAt")]
    public DateTime CreAt { get; set; } = DateTime.Now;

    /// <summary>
    /// 最終更新日時（メタ情報）。
    /// </summary>
    [JsonPropertyName("updAt")]
    public DateTime UpdAt { get; set; } = DateTime.Now;

    /// <summary>
    /// 重複検知用のキー（内部利用、シリアライズ除外）。
    /// </summary>
    [JsonIgnore]
    public string IdemKey { get; set; } = string.Empty;

    /// <summary>
    /// S3 バケット名（ローカル実行時の既定値あり）。
    /// </summary>
    [JsonIgnore]
    public string S3Bucket { get; set; } = "local-development-bucket";

    /// <summary>
    /// オブジェクトの ETag（S3 等）
    /// </summary>
    [JsonIgnore]
    public string Etag { get; set; } = string.Empty;

    /// <summary>
    /// データ種別（PRODUCT, EVENT 等）。
    /// </summary>
    [JsonIgnore]
    public string DataKind { get; set; } = "PRODUCT";

    /// <summary>
    /// ストレージ上のファイルキー（内部利用、シリアライズ除外）。
    /// </summary>
    [JsonIgnore]
    public string FileKey { get; set; } = string.Empty;

    /// <summary>
    /// 内部的なバッチ状態（UI/ログ向け）。
    /// </summary>
    [JsonIgnore]
    public string BatchStatus { get; set; } = "RUNNING";

    /// <summary>
    /// 件数の集計をJSONで保持するフィールド（内部利用）。
    /// </summary>
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