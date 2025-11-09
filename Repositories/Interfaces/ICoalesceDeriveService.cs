using System.Threading.Tasks;
using ProductDataIngestion.Models;

namespace ProductDataIngestion.Services.Interfaces
{
    /// <summary>
    /// DERIVE_COALESCE 型の属性派生処理を行うサービス。
    /// 現時点では未実装。
    /// </summary>
    public interface ICoalesceDeriveService
    {
        /// <summary>
        /// DERIVE_COALESCE ルールに基づいて値を派生する。
        /// </summary>
        /// <param name="attr">処理対象の属性</param>
        /// <returns>派生結果（現時点では null を返す）</returns>
        Task<(string? ValueCd, string? ValueText)> DeriveAsync(ClProductAttr attr);
    }
}
