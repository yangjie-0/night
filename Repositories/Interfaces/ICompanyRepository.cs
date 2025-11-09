using ProductDataIngestion.Models;

namespace ProductDataIngestion.Repositories.Interfaces
{
    /// <summary>
    /// GP会社コードに紐づく会社情報を取得するためのリポジトリインターフェース。
    /// </summary>
    public interface ICompanyRepository
    {
        /// <summary>
        /// 指定したGP会社コードに対応するアクティブな会社情報を取得する。存在しない場合はnull。
        /// </summary>
        Task<MCompany?> GetActiveCompanyAsync(string groupCompanyCd);
    }
}
