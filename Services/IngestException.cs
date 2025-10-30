using ProductDataIngestion.Models;

namespace ProductDataIngestion.Services
{
    /// <summary>
    /// Ingest処理のカスタム例外クラス
    /// ErrorDetailモデルを使用してエラー情報を構造化
    /// </summary>
    public class IngestException : Exception
    {
        /// <summary>
        /// エラー詳細情報
        /// </summary>
        public ErrorDetail ErrorDetail { get; }

        /// <summary>
        /// コンストラクタ (ErrorDetail使用)
        /// </summary>
        /// <param name="errorDetail">エラー詳細情報</param>
        // public IngestException(ErrorDetail errorDetail)
        //     : base(errorDetail.Message)
        // {
        //     ErrorDetail = errorDetail;
        // }

        /// <summary>
        /// コンストラクタ (ErrorDetail + 内部例外)
        /// </summary>
        /// <param name="errorDetail">エラー詳細情報</param>
        /// <param name="innerException">内部例外</param>
        // public IngestException(ErrorDetail errorDetail, Exception innerException)
        //     : base(errorDetail.Message, innerException)
        // {
        //     ErrorDetail = errorDetail;
        // }

        /// <summary>
        /// コンストラクタ (簡易版 - 下位互換性のため)
        /// </summary>
        /// <param name="errorCode">エラーコード</param>
        /// <param name="message">エラーメッセージ</param>
        /// <param name="recordRef">レコード参照</param>
        /// <param name="rawFragment">生データ断片</param>
        public IngestException(string errorCode, string message, string? recordRef = null, string? rawFragment = null)
            : base(message)
        {
            ErrorDetail = new ErrorDetail(errorCode, message, recordRef, rawFragment);
        }

        /// <summary>
        /// コンストラクタ (簡易版 + 内部例外 - 下位互換性のため)
        /// </summary>
        /// <param name="errorCode">エラーコード</param>
        /// <param name="message">エラーメッセージ</param>
        /// <param name="innerException">内部例外</param>
        /// <param name="recordRef">レコード参照</param>
        /// <param name="rawFragment">生データ断片</param>
        public IngestException(string errorCode, string message, Exception innerException, string? recordRef = null, string? rawFragment = null)
            : base(message, innerException)
        {
            ErrorDetail = new ErrorDetail(errorCode, message, recordRef, rawFragment);
        }

        /// <summary>
        /// 便利プロパティ: ErrorCode (下位互換性のため)
        /// </summary>
        public string ErrorCode => ErrorDetail.ErrorCode;

        /// <summary>
        /// 便利プロパティ: RecordRef (下位互換性のため)
        /// </summary>
        public string? RecordRef => ErrorDetail.RecordRef;

        /// <summary>
        /// 便利プロパティ: RawFragment (下位互換性のため)
        /// </summary>
        public string? RawFragment => ErrorDetail.RawFragment;

        /// <summary>
        /// 例外の文字列表現
        /// </summary>
        // public override string ToString()
        // {
        //     return $"{ErrorDetail}\n{base.ToString()}";
        // }
    }
}
