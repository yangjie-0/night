namespace ProductDataIngestion.Models
{
    /// <summary>
    /// 一時的に解析した商品データを保持するモデル（ライン単位）。
    /// CSVや外部ソースから読み込んだ原始値（Source*）を格納し、クレンジング対象として利用される。
    /// </summary>
    public class TempProductParsed
    {
        /// <summary>
        /// 一時行の識別子（GUID）。
        /// </summary>
        public Guid TempRowId { get; set; }

        /// <summary>
        /// 関連するバッチID。
        /// </summary>
        public string BatchId { get; set; } = string.Empty;

        /// <summary>
        /// 元ファイル上の行番号。
        /// </summary>
        public long LineNo { get; set; }

        /// <summary>
        /// 元データの所属グループ会社コード。
        /// </summary>
        public string SourceGroupCompanyCd { get; set; } = string.Empty;

        // 基本商品情報
        /// <summary>元データの商品コード。</summary>
        public string? SourceProductCd { get; set; }
        /// <summary>元データの商品管理コード。</summary>
        public string? SourceProductManagementCd { get; set; }
        /// <summary>元データのブランドID。</summary>
        public string? SourceBrandId { get; set; }
        /// <summary>元データのブランド名。</summary>
        public string? SourceBrandNm { get; set; }

        // 分類情報
        /// <summary>カテゴリ1のID。</summary>
        public string? SourceCategory1Id { get; set; }
        /// <summary>カテゴリ1の名前。</summary>
        public string? SourceCategory1Nm { get; set; }
        /// <summary>カテゴリ2のID。</summary>
        public string? SourceCategory2Id { get; set; }
        /// <summary>カテゴリ2の名前。</summary>
        public string? SourceCategory2Nm { get; set; }
        /// <summary>カテゴリ3のID。</summary>
        public string? SourceCategory3Id { get; set; }
        /// <summary>カテゴリ3の名前。</summary>
        public string? SourceCategory3Nm { get; set; }

        // 商品状態
        /// <summary>元データの商品ステータスコード。</summary>
        public string? SourceProductStatusCd { get; set; }
        /// <summary>元データの商品ステータス名。</summary>
        public string? SourceProductStatusNm { get; set; }
        /// <summary>新品/中古区分など。</summary>
        public string? SourceNewUsedKbn { get; set; }
        /// <summary>数量（元データの文字列）。</summary>
        public string? SourceQuantity { get; set; }

        // 在庫情報
        /// <summary>在庫有無コード。</summary>
        public string? SourceStockExistenceCd { get; set; }
        /// <summary>在庫有無の表示名。</summary>
        public string? SourceStockExistenceNm { get; set; }
        /// <summary>販売可否コード。</summary>
        public string? SourceSalePermissionCd { get; set; }
        /// <summary>販売可否の表示名。</summary>
        public string? SourceSalePermissionNm { get; set; }

        // 状態情報
        public string? SourceTransferStatus { get; set; }
        public string? SourceRepairStatus { get; set; }
        public string? SourceReservationStatus { get; set; }
        public string? SourceConsignmentStatus { get; set; }
        public string? SourceAcceptStatus { get; set; }
        public string? SourceEcListingKbn { get; set; }

        // 価格情報
        public string? SourceAssessmentPriceExclTax { get; set; }
        public string? SourceAssessmentPriceInclTax { get; set; }
        public string? SourceAssessmentTaxRate { get; set; }
        public string? SourcePurchasePriceExclTax { get; set; }
        public string? SourcePurchasePriceInclTax { get; set; }
        public string? SourcePurchaseTaxRate { get; set; }
        public string? SourceDisplayPriceExclTax { get; set; }
        public string? SourceDisplayPriceInclTax { get; set; }
        public string? SourceDisplayTaxRate { get; set; }
        public string? SourceSalesPriceExclTax { get; set; }
        public string? SourceSalesPriceInclTax { get; set; }
        public string? SourceSalesTaxRate { get; set; }

        // 排名信息 (ランキング等)
        public string? SourcePurchaseRank { get; set; }
        public string? SourcePurchaseRankNm { get; set; }
        public string? SourceSalesRank { get; set; }
        public string? SourceSalesRankNm { get; set; }

        // チャネル/経路情報
        public string? SourceSalesChannelNm { get; set; }
        public string? SourceSalesChannelRegion { get; set; }
        public string? SourceSalesChannelMethod { get; set; }
        public string? SourceSalesChannelTarget { get; set; }
        public string? SourcePurchaseChannelNm { get; set; }
        public string? SourcePurchaseChannelRegion { get; set; }
        public string? SourcePurchaseChannelMethod { get; set; }
        public string? SourcePurchaseChannelTarget { get; set; }

        // 店舗情報
        public string? SourceStoreId { get; set; }
        public string? SourceStoreNm { get; set; }

        // 委託情報
        public string? SourceConsignorGroupCompanyId { get; set; }
        public string? SourceConsignorProductCd { get; set; }

        /// <summary>
        /// パイプライン上のステップ状態（READY, PROCESSING, DONE 等）。
        /// </summary>
        public string StepStatus { get; set; } = "READY";

        /// <summary>
        /// 追加情報を格納する JSON（任意）。
        /// </summary>
        public string ExtrasJson { get; set; } = "{}";
    }
}