namespace ProductDataIngestion.Models
{
    /// <summary>
    /// レコード単位のエラー情報を保持するモデル。
    /// テーブル: record_error
    /// </summary>
    public class RecordError
    {
        /// <summary>
        /// エラーID（主キー）
        /// </summary>
        public Guid ErrorId { get; set; }

        /// <summary>
        /// バッチID
        /// </summary>
        public string BatchId { get; set; } = string.Empty;

        /// <summary>
        /// 処理ステップ名（例: INGEST, CLEANSE）
        /// </summary>
        public string Step { get; set; } = string.Empty;

        /// <summary>
        /// レコード参照（行番号や一時行IDなど）
        /// </summary>
        public string RecordRef { get; set; } = string.Empty;

        /// <summary>
        /// エラーコード
        /// </summary>
        public string ErrorCd { get; set; } = string.Empty;

        /// <summary>
        /// エラーの詳細（人間が理解できる説明）
        /// </summary>
        public string ErrorDetail { get; set; } = string.Empty;

        /// <summary>
        /// 元データの断片（該当CSV行やJSON抜粋）
        /// </summary>
        public string RawFragment { get; set; } = string.Empty;

        /// <summary>
        /// 登録日時 (cre_at)
        /// </summary>
        public DateTime CreAt { get; set; } = DateTime.UtcNow;

        /// <summary>
        /// 更新日時 (upd_at)
        /// </summary>
        public DateTime UpdAt { get; set; } = DateTime.UtcNow;
    }
}