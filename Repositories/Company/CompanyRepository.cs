using Dapper;
using Npgsql;
using ProductDataIngestion.Models;
using ProductDataIngestion.Repositories.Interfaces;

namespace ProductDataIngestion.Repositories.Company
{
    /// <summary>
    /// m_companyテーブルを参照し、GP会社情報を取得するリポジトリ。
    /// </summary>
    public class CompanyRepository : ICompanyRepository
    {
        private readonly string _connectionString;

        public CompanyRepository(string connectionString)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        }

        /// <summary>
        /// 指定GP会社コードに該当するアクティブな会社情報を取得する。
        /// 該当レコードが無い場合は null を返す。
        /// </summary>
        public async Task<MCompany?> GetActiveCompanyAsync(string groupCompanyCd)
        {
            using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync();

            const string sql = @"
                SELECT group_company_id as GroupCompanyId, group_company_cd as GroupCompanyCd,
                       group_company_nm as GroupCompanyNm, default_currency_cd as DefaultCurrencyCd,
                       is_active as IsActive, cre_at as CreAt, upd_at as UpdAt
                FROM m_company
                WHERE group_company_cd = @GroupCompanyCd AND is_active = true";

            return await connection.QueryFirstOrDefaultAsync<MCompany>(
                sql,
                new { GroupCompanyCd = groupCompanyCd }
            );
        }
    }
}
