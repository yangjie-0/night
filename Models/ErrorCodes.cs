namespace ProductDataIngestion.Models
{
    /// <summary>
    /// エラーコード定数クラス
    /// 設計書に基づくエラー分類コード
    /// </summary>
    public static class ErrorCodes
    {
        #region CSV解析エラー

        /// <summary>
        /// CSV解析失敗
        /// </summary>
        public const string PARSE_FAILED = "PARSE_FAILED";

        /// <summary>
        /// 文字コードエラー
        /// </summary>
        public const string INVALID_ENCODING = "INVALID_ENCODING";

        /// <summary>
        /// 行サイズ超過
        /// </summary>
        public const string ROW_TOO_LARGE = "ROW_TOO_LARGE";

        #endregion

        #region データ検証エラー

        /// <summary>
        /// 必須列エラー
        /// </summary>
        public const string MISSING_COLUMN = "MISSING_COLUMN";

        /// <summary>
        /// 空レコードエラー
        /// </summary>
        public const string EMPTY_RECORD = "EMPTY_RECORD";

        /// <summary>
        /// 必須フィールド空エラー
        /// </summary>
        public const string REQUIRED_FIELD_EMPTY = "REQUIRED_FIELD_EMPTY";

        #endregion

        #region 型変換エラー

        /// <summary>
        /// 数値型変換エラー
        /// </summary>
        public const string CAST_NUM_FAILED = "CAST_NUM_FAILED";

        /// <summary>
        /// 日付型変換エラー
        /// </summary>
        public const string CAST_DATE_FAILED = "CAST_DATE_FAILED";

        /// <summary>
        /// 真偽値型変換エラー
        /// </summary>
        public const string CAST_BOOL_FAILED = "CAST_BOOL_FAILED";

        #endregion

        #region マッピングエラー

        /// <summary>
        /// マッピング定義未発見
        /// </summary>
        public const string MAPPING_NOT_FOUND = "MAPPING_NOT_FOUND";

        #endregion

        #region データベースエラー

        /// <summary>
        /// データベースエラー
        /// </summary>
        public const string DB_ERROR = "DB_ERROR";

        #endregion

        #region ファイル操作エラー

        /// <summary>
        /// S3ファイル移動失敗
        /// </summary>
        public const string S3_MOVE_FAILED = "S3_MOVE_FAILED";

        /// <summary>
        /// ローカルファイル移動失敗
        /// </summary>
        public const string LOCAL_MOVE_FAILED = "LOCAL_MOVE_FAILED";

        #endregion
    }
}
