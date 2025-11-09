using System;

namespace ProductDataIngestion.Models
{
    /// <summary>
    /// 製品マスタを表すモデル（m_product_management）。
    /// KM会社のみUPSERT時に生成され、source_product_management_cdをキーとする。
    /// </summary>
    public class MProductManagement
    {
        /// <summary>
        /// 製品ID（主キー）
        /// </summary>
        public long GProductManagementId { get; set; }

        /// <summary>
        /// GP会社ID
        /// </summary>
        public long GroupCompanyId { get; set; }

        /// <summary>
        /// 連携元製品コード（KM製品コード）
        /// </summary>
        public string SourceProductManagementCd { get; set; } = string.Empty;

        /// <summary>
        /// Gブランドコード
        /// </summary>
        public long? GBrandId { get; set; }

        /// <summary>
        /// GカテゴリID（葉）- 必須フィールド
        /// </summary>
        public long GCategoryId { get; set; }

        /// <summary>
        /// 製品説明（要約・代表）、m_product_eav.CATALOG_DESCから取得
        /// </summary>
        public string? DescriptionText { get; set; }

        /// <summary>
        /// 仮製品フラグ（KMの場合はFALSE）
        /// </summary>
        public bool IsProvisional { get; set; }

        /// <summary>
        /// 仮製品元商品
        /// </summary>
        public long? SourceProductCd { get; set; }

        /// <summary>
        /// 由来情報（JSON）
        /// </summary>
        public string? ProvenanceJson { get; set; }

        /// <summary>
        /// 最終更新に関わったバッチID
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
