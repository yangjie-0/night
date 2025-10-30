using System;

namespace ProductDataIngestion.Models
{
    /// <summary>
    /// 固定列から属性へのマッピング定義モデル。
    /// 固定列（CSVのカラム）をどの属性にマップするか等のルールを保持する。
    /// </summary>
    public class MFixedToAttrMap
    {
        /// <summary>
        /// マップ定義の内部ID。
        /// </summary>
        public long MapId { get; set; }

        /// <summary>
        /// グループ会社コード。
        /// </summary>
        public string GroupCompanyCd { get; set; } = string.Empty;

        /// <summary>
        /// データ種別（PRODUCT など）。
        /// </summary>
        public string ProjectionKind { get; set; } = string.Empty;

        /// <summary>
        /// マッピング先の属性コード。
        /// </summary>
        public string AttrCd { get; set; } = string.Empty;

        /// <summary>
        /// 元データのIDカラム名。
        /// </summary>
        public string SourceIdColumn { get; set; } = string.Empty;

        /// <summary>
        /// 元データのラベルカラム名。
        /// </summary>
        public string SourceLabelColumn { get; set; } = string.Empty;

        /// <summary>
        /// 値の役割（例: メイン値、補足値など）。
        /// </summary>
        public string ValueRole { get; set; } = string.Empty;

        /// <summary>
        /// データタイプ上書き（必要な場合）。
        /// </summary>
        public string DataTypeOverride { get; set; } = string.Empty;

        /// <summary>
        /// 分割モード（必要に応じて複数値を分割する設定）。
        /// </summary>
        public string SplitMode { get; set; } = string.Empty;

        /// <summary>
        /// このマッピングが有効かどうか。
        /// </summary>
        public bool IsActive { get; set; }

        /// <summary>
        /// 優先度（複数マッピングがある場合に使用）。
        /// </summary>
        public int Priority { get; set; }

        /// <summary>
        /// 備考。
        /// </summary>
        public string FixedRemarks { get; set; } = string.Empty;
    }
}