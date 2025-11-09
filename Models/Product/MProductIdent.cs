using System;

namespace ProductDataIngestion.Models
{
    /// <summary>
    /// m_product_ident テーブルを表すモデル。
    /// 商品識別時の検索・登録に利用する。
    /// </summary>
    // シンプル説明: 製品の同定キー(source_product_cd 等)を管理する識別レコード。
    public class MProductIdent
    {
        public long IdentId { get; set; }
        public long GProductId { get; set; }
        public long GroupCompanyId { get; set; }
        public string? SourceProductCd { get; set; }
        public string? SourceProductManagementCd { get; set; }
        public string IdentKind { get; set; } = "AUTO";
        public decimal Confidence { get; set; } = 1.0m;
        public bool IsPrimary { get; set; } = true;
        public bool IsActive { get; set; } = true;
        public DateTime ValidFrom { get; set; } = DateTime.UtcNow;
        public DateTime? ValidTo { get; set; }
        public string ProvenanceJson { get; set; } = "{}";
        public string? IdentRemarks { get; set; }
        public string? BatchId { get; set; }
        public DateTime CreAt { get; set; }
        public DateTime UpdAt { get; set; }
    }
}
