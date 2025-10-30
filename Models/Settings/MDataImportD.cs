namespace ProductDataIngestion.Models
{
    /// <summary>
    /// データ取り込み時のカラム定義（プロファイル詳細）を表すモデル。
    /// 各カラムがどの属性にマップされるか、変換式やキャスト情報を保持する。
    /// </summary>
    public class MDataImportD
    {
        /// <summary>
        /// プロファイルID。
        /// </summary>
        public long ProfileId { get; set; }

        /// <summary>
        /// カラムのシーケンス番号。
        /// </summary>
        public int ColumnSeq { get; set; }

        /// <summary>
        /// 投影種別（例: 固定列、可変列などの区分）。
        /// </summary>
        public string ProjectionKind { get; set; } = string.Empty;

        /// <summary>
        /// マッピング先の属性コード。
        /// </summary>
        public string AttrCd { get; set; } = string.Empty;

        /// <summary>
        /// マッピング先のターゲットカラム名。
        /// </summary>
        public string TargetColumn { get; set; } = string.Empty;

        /// <summary>
        /// キャストタイプ（必要な場合）。
        /// </summary>
        public string CastType { get; set; } = string.Empty;        // 新增

        /// <summary>
        /// 変換式（Transform expression）。
        /// </summary>
        public string TransformExpr { get; set; } = string.Empty;   // 新增

        /// <summary>
        /// 必須フラグ。
        /// </summary>
        public bool IsRequired { get; set; }
    }
}