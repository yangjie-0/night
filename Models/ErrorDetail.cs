namespace ProductDataIngestion.Models
{
    /// <summary>
    /// エラー詳細情報モデル
    /// エラー発生時の詳細情報を構造化して保持
    /// </summary>
    public class ErrorDetail
    {
        /// <summary>
        /// エラーコード (ErrorCodesクラスの定数を使用)
        /// 例: "PARSE_FAILED", "MISSING_COLUMN"
        /// </summary>
        public string ErrorCode { get; set; } = string.Empty;

        /// <summary>
        /// エラーメッセージ
        /// </summary>
        public string Message { get; set; } = string.Empty;

        /// <summary>
        /// レコード参照 (行番号、ID、ファイルパスなど)
        /// 例: "Row 123", "ProductId: ABC-001"
        /// </summary>
        public string? RecordRef { get; set; }

        /// <summary>
        /// エラー発生時の生データ断片
        /// デバッグやトラブルシューティング用
        /// </summary>
        public string? RawFragment { get; set; }

        /// <summary>
        /// エラー発生時刻 (オプション)
        /// </summary>
        public DateTime? Timestamp { get; set; }

        /// <summary>
        /// 追加コンテキスト情報 (オプション)
        /// 例: ファイル名、処理ステップなど
        /// </summary>
        public Dictionary<string, string>? Context { get; set; }

        /// <summary>
        /// コンストラクタ
        /// </summary>
        public ErrorDetail()
        {
        }

        /// <summary>
        /// コンストラクタ (基本情報)
        /// </summary>
        /// <param name="errorCode">エラーコード</param>
        /// <param name="message">エラーメッセージ</param>
        /// <param name="recordRef">レコード参照</param>
        /// <param name="rawFragment">生データ断片</param>
        public ErrorDetail(string errorCode, string message, string? recordRef = null, string? rawFragment = null)
        {
            ErrorCode = errorCode;
            Message = message;
            RecordRef = recordRef;
            RawFragment = rawFragment;
            Timestamp = DateTime.UtcNow;
        }

        /// <summary>
        /// エラー詳細の文字列表現
        /// </summary>
        public override string ToString()
        {
            var parts = new List<string> { $"[{ErrorCode}] {Message}" };

            if (!string.IsNullOrEmpty(RecordRef))
                parts.Add($"RecordRef: {RecordRef}");

            if (!string.IsNullOrEmpty(RawFragment))
                parts.Add($"RawData: {RawFragment}");

            return string.Join(" | ", parts);
        }
    }
}
