using System;
namespace ProductDataIngestion.Models
{
    /// <summary>
    /// 企業（グループ会社）情報を表すモデル。
    /// グループ会社コード、名称、通貨など管理情報を保持する。
    /// </summary>
    public class MCompany
    {
        /// <summary>
        /// グループ会社の内部ID。
        /// </summary>
        public string GroupCompanyId { get; set; } = string.Empty;

        /// <summary>
        /// グループ会社コード。
        /// </summary>
        public string GroupCompanyCd { get; set; } = string.Empty;

        /// <summary>
        /// グループ会社名。
        /// </summary>
        public string GroupCompanyNm { get; set; } = string.Empty;

        /// <summary>
        /// 既定通貨コード。
        /// </summary>
        public string DefaultCurrencyCd { get; set; } = string.Empty;

        /// <summary>
        /// 有効フラグ。
        /// </summary>
        public bool IsActive { get; set; }

        /// <summary>
        /// 作成日時。
        /// </summary>
        public DateTime CreAt { get; set; }

        /// <summary>
        /// 更新日時。
        /// </summary>
        public DateTime UpdAt { get; set; }

        /// <summary>
        /// 基本的な妥当性チェック。
        /// </summary>
        public bool IsValid()
        {
            return !string.IsNullOrWhiteSpace(GroupCompanyCd) && 
                   !string.IsNullOrWhiteSpace(GroupCompanyNm) && 
                   IsActive;
        }
    }
}