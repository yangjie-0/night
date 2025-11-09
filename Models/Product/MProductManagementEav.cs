using System;

namespace ProductDataIngestion.Models
{
    /// <summary>
    /// 製品EAVマスタを表すモデル（m_product_management_eav）。
    /// 製品に関連する動的な属性を保持する。
    /// </summary>
    public class MProductManagementEav
    {
        /// <summary>
        /// 製品ID
        /// </summary>
        public long GProductManagementId { get; set; }

        /// <summary>
        /// 項目コード
        /// </summary>
        public string AttrCd { get; set; } = string.Empty;

        /// <summary>
        /// 順序（同じ項目が複数ある場合の順序番号）
        /// </summary>
        public short AttrSeq { get; set; } = 1;

        /// <summary>
        /// 属性値（文字列）
        /// </summary>
        public string? ValueText { get; set; }

        /// <summary>
        /// 属性値（数値）
        /// </summary>
        public decimal? ValueNum { get; set; }

        /// <summary>
        /// 属性値（日付）
        /// </summary>
        public DateTime? ValueDate { get; set; }

        /// <summary>
        /// 属性値（コード値）
        /// </summary>
        public string? ValueCd { get; set; }

        /// <summary>
        /// 単位コード
        /// </summary>
        public string? UnitCd { get; set; }

        /// <summary>
        /// クレンジング品質フラグ
        /// </summary>
        public string? QualityStatus { get; set; }

        /// <summary>
        /// クレンジング詳細情報（JSON）
        /// </summary>
        public string? QualityDetailJson { get; set; }

        /// <summary>
        /// 属性由来情報（JSON）
        /// </summary>
        public string? ProvenanceJson { get; set; }

        /// <summary>
        /// バッチID
        /// </summary>
        public string? BatchId { get; set; }

        /// <summary>
        /// 有効フラグ
        /// </summary>
        public bool IsActive { get; set; } = true;

        /// <summary>
        /// 登録日時
        /// </summary>
        public DateTime CreAt { get; set; }

        /// <summary>
        /// 更新日時
        /// </summary>
        public DateTime UpdAt { get; set; }
    }
}
