namespace ProductDataIngestion.Services
{
    /// <summary>
    /// Ingest処理のカスタム例外クラス
    /// 設計書に基づいたエラー分類コードを保持
    /// </summary>
    public class IngestException : Exception
    {
        public string ErrorCode { get; set; }
        public string RecordRef { get; set; }
        public string RawFragment { get; set; }

        public IngestException(string errorCode, string message, string recordRef = "", string rawFragment = "")
            : base(message)
        {
            ErrorCode = errorCode;
            RecordRef = recordRef;
            RawFragment = rawFragment;
        }

        public IngestException(string errorCode, string message, Exception innerException, string recordRef = "", string rawFragment = "")
            : base(message, innerException)
        {
            ErrorCode = errorCode;
            RecordRef = recordRef;
            RawFragment = rawFragment;
        }
    }

    /// <summary>
    /// エラーコード定数 (設計書に基づく)
    /// </summary>
    public static class ErrorCodes
    {
        // CSV解析エラー
        public const string PARSE_FAILED = "PARSE_FAILED";

        // 必須列エラー
        public const string MISSING_COLUMN = "MISSING_COLUMN";

        // 空レコードエラー
        public const string EMPTY_RECORD = "EMPTY_RECORD";

        // 必須フィールド空エラー
        public const string REQUIRED_FIELD_EMPTY = "REQUIRED_FIELD_EMPTY";

        // 文字コードエラー
        public const string INVALID_ENCODING = "INVALID_ENCODING";

        // 行サイズ超過
        public const string ROW_TOO_LARGE = "ROW_TOO_LARGE";

        // 型変換エラー
        public const string CAST_NUM_FAILED = "CAST_NUM_FAILED";
        public const string CAST_DATE_FAILED = "CAST_DATE_FAILED";
        public const string CAST_BOOL_FAILED = "CAST_BOOL_FAILED";

        // マッピングエラー
        public const string MAPPING_NOT_FOUND = "MAPPING_NOT_FOUND";

        // データベースエラー
        public const string DB_ERROR = "DB_ERROR";

        // ファイル移動エラー
        public const string S3_MOVE_FAILED = "S3_MOVE_FAILED";
        public const string LOCAL_MOVE_FAILED = "LOCAL_MOVE_FAILED";
    }
}
