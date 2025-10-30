namespace ProductDataIngestion.Models
{
    /// <summary>
    /// 発生したエラーの詳細を表すモデル。
    /// エラーコード、メッセージ、参照レコードや生データ断片などを保持する。
    /// デバッグやログ、エラー報告に利用する。
    /// </summary>
    public class ErrorDetail
    {
    /// <summary>
    /// エラーコード（`ErrorCodes` 定義の定数を使用）。例: "PARSE_FAILED"。
    /// </summary>
    public string ErrorCode { get; set; } = string.Empty;

    /// <summary>
    /// 人間向けのエラーメッセージ。
    /// </summary>
    public string Message { get; set; } = string.Empty;

    /// <summary>
    /// エラーが発生したレコードの参照情報（行番号やID、識別子）。
    /// </summary>
    public string? RecordRef { get; set; }

    /// <summary>
    /// 生データの断片（エラー発生箇所の抜粋）。デバッグに使用。
    /// </summary>
    public string? RawFragment { get; set; }

    /// <summary>
    /// エラー発生のタイムスタンプ（UTC）。オプション。
    /// </summary>
    public DateTime? Timestamp { get; set; }

    /// <summary>
    /// 追加のコンテキスト情報（キー/値）。ファイル名や処理ステップなど補助情報を格納する。
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
        /// エラー詳細の文字列表現（現時点未使用）
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
