using ProductDataIngestion.Models;

namespace ProductDataIngestion.Services
{
    /// <summary>
    /// Import処理のカスタム例外クラス
    /// データインポート処理で発生するエラーを統一的に処理
    /// </summary>
    public class ImportException : Exception
    {
        /// <summary>
        /// エラー詳細情報
        /// </summary>
        public ErrorDetail ErrorDetail { get; }

        /// <summary>
        /// コンストラクタ (ErrorDetail使用)
        /// </summary>
        /// <param name="errorDetail">エラー詳細情報</param>
        public ImportException(ErrorDetail errorDetail)
            : base(errorDetail.Message)
        {
            ErrorDetail = errorDetail;
        }

        /// <summary>
        /// コンストラクタ (ErrorDetail + 内部例外)
        /// </summary>
        /// <param name="errorDetail">エラー詳細情報</param>
        /// <param name="innerException">内部例外</param>
        public ImportException(ErrorDetail errorDetail, Exception innerException)
            : base(errorDetail.Message, innerException)
        {
            ErrorDetail = errorDetail;
        }

        /// <summary>
        /// コンストラクタ (簡易版 - メッセージのみ)
        /// </summary>
        /// <param name="message">エラーメッセージ</param>
        public ImportException(string message)
            : base(message)
        {
            ErrorDetail = new ErrorDetail(ErrorCodes.DB_ERROR, message);
        }

        /// <summary>
        /// コンストラクタ (簡易版 + 内部例外)
        /// </summary>
        /// <param name="message">エラーメッセージ</param>
        /// <param name="innerException">内部例外</param>
        public ImportException(string message, Exception innerException)
            : base(message, innerException)
        {
            ErrorDetail = new ErrorDetail(ErrorCodes.DB_ERROR, message);
        }

        /// <summary>
        /// 例外の文字列表現
        /// </summary>
        public override string ToString()
        {
            return $"{ErrorDetail}\n{base.ToString()}";
        }
    }
}
