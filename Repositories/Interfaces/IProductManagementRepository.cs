using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using ProductDataIngestion.Models;

namespace ProductDataIngestion.Repositories.Interfaces
{
    public interface IProductManagementRepository
    {
        /// <summary>
        /// 指定されたgroup_company_idとsource_product_management_cdに基づいて製品管理IDを検索
        /// </summary>
        Task<long?> FindActiveProductManagementIdAsync(
            NpgsqlConnection connection,
            long groupCompanyId,
            string sourceProductManagementCd,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken);

        /// <summary>
        /// 次の製品管理IDを取得
        /// </summary>
        Task<long> GetNextProductManagementIdAsync(
            NpgsqlConnection connection,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken);

        /// <summary>
        /// 製品管理レコードを取得
        /// </summary>
        Task<MProductManagement?> GetProductManagementAsync(
            NpgsqlConnection connection,
            long gProductManagementId,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken);

        /// <summary>
        /// 製品管理レコードを挿入
        /// </summary>
        Task InsertProductManagementAsync(
            NpgsqlConnection connection,
            MProductManagement entity,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken);

        /// <summary>
        /// 製品管理レコードを更新
        /// </summary>
        Task UpdateProductManagementAsync(
            NpgsqlConnection connection,
            long gProductManagementId,
            IDictionary<string, object?> values,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken);

        /// <summary>
        /// 製品管理EAVマップを取得
        /// </summary>
        Task<Dictionary<(string attrCd, short attrSeq), MProductManagementEav>> GetProductManagementEavMapAsync(
            NpgsqlConnection connection,
            long gProductManagementId,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken);

        /// <summary>
        /// 製品管理EAVレコードを挿入
        /// </summary>
        Task InsertProductManagementEavAsync(
            NpgsqlConnection connection,
            MProductManagementEav entity,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken);

        /// <summary>
        /// 製品管理EAVレコードを更新
        /// </summary>
        Task UpdateProductManagementEavAsync(
            NpgsqlConnection connection,
            long gProductManagementId,
            string attrCd,
            short attrSeq,
            IDictionary<string, object?> values,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken);

        /// <summary>
        /// 製品管理EAVを非アクティブにマーク
        /// </summary>
        Task MarkProductManagementEavInactiveAsync(
            NpgsqlConnection connection,
            long gProductManagementId,
            string attrCd,
            short attrSeq,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken);
    }
}
