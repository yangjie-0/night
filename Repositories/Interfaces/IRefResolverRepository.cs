// ProductDataIngestion.Repositories/IRefResolverRepository.cs
using System.Threading.Tasks;
using ProductDataIngestion.Models;

namespace ProductDataIngestion.Repositories.Interfaces
{
    public interface IRefResolverRepository
    {
        /// <summary>
        /// m_ref_table_map の定義に基づき、共通SQLで value_cd / value_text を解決する
        /// </summary>
        Task<(string? ValueCd, string? ValueText)> ResolveAsync(
            RefTableMap refMap,
            string? sourceId,
            string? sourceLabel
        );
    }
}
