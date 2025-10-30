namespace ProductDataIngestion.Models
{
    /// <summary>
    /// レコード単位のエラー情報を保持する簡易モデル。
    /// バッチID、処理ステップ、参照情報、エラーコード/詳細、生データ断片を含む。
    /// </summary>
    public class RecordError
    {
        /// <summary>
        /// 関連するバッチID。
        /// </summary>
        public string BatchId { get; set; } = string.Empty;

        /// <summary>
        /// 処理ステップ名（例: INGEST, CLEANSE）。
        /// </summary>
        public string Step { get; set; } = string.Empty;

        /// <summary>
        /// レコード参照（行番号やID）。
        /// </summary>
        public string RecordRef { get; set; } = string.Empty;

        /// <summary>
        /// エラーコード。
        /// </summary>
        public string ErrorCd { get; set; } = string.Empty;

        /// <summary>
        /// エラーの詳細説明。
        /// </summary>
        public string ErrorDetail { get; set; } = string.Empty;

        /// <summary>
        /// 生データの断片。
        /// </summary>
        public string RawFragment { get; set; } = string.Empty;
    }
}