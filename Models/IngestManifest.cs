using System;
using System.Collections.Generic;

namespace ProductDataIngestion.Models
{
    /// <summary>
    /// 取り込み（Ingest）時のマニフェスト情報を保持するモデル。
    /// S3 のバケット/オブジェクト情報や行数、メタ情報を格納する。
    /// </summary>
    public class IngestManifest
    {
        /// <summary>
        /// 関連するバッチID。
        /// </summary>
        public string BatchId { get; set; } = string.Empty;

        /// <summary>
        /// S3 バケット名。
        /// </summary>
        public string S3Bucket { get; set; } = string.Empty;

        /// <summary>
        /// S3 オブジェクトキー（またはファイルパス）。
        /// </summary>
        public string ObjectKey { get; set; } = string.Empty;

        /// <summary>
        /// オブジェクトの ETag（整合性確認用）。
        /// </summary>
        public string ETag { get; set; } = string.Empty;

        /// <summary>
        /// ファイルの行数。
        /// </summary>
        public int RowCount { get; set; }

        /// <summary>
        /// 任意のメタ情報を格納する JSON 文字列。
        /// </summary>
        public string MetaJson { get; set; } = "{}";

        /// <summary>
        /// マニフェスト作成日時。
        /// </summary>
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

        /// <summary>
        /// 最終更新日時。
        /// </summary>
        public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
    }
}