using System;

namespace ProductDataIngestion.Models
{
    /// <summary>
    /// m_product テーブルに対応するモデル。
    /// UPSERT 処理で固定列の比較や更新に利用する。
    /// </summary>
    // シンプル説明: 固定カラム(ブランド/カテゴリ/価格など)を保持する商品レコードの本体。
    public class MProduct
    {
        public long GProductId { get; set; }
        public string GProductCd { get; set; } = string.Empty;
        public int UnitNo { get; set; }
        public long GroupCompanyId { get; set; }
        public string? SourceProductCd { get; set; }
        public string? SourceProductManagementCd { get; set; }
        public long? GBrandId { get; set; }
        public long? GCategoryId { get; set; }
        public string? CurrencyCd { get; set; }
        public decimal? DisplayPriceInclTax { get; set; }
        public string ProductStatusCd { get; set; } = "PRODUCT_STATUS_UNKNOWN";
        public string NewUsedKbnCd { get; set; } = "PRODUCT_CONDITION_UNKNOWN";
        public string StockExistenceCd { get; set; } = "STOCK_UNKNOWN";
        public string SaleStatusCd { get; set; } = "SALE_UNKNOWN";
        public DateTime? LastEventTs { get; set; }
        public string? LastEventKindCd { get; set; }
        public bool IsActive { get; set; } = true;
        public DateTime CreAt { get; set; }
        public DateTime UpdAt { get; set; }
    }
}
