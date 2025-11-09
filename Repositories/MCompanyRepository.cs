using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ProductDataIngestion.Models;
using ProductDataIngestion.Repositories.Interfaces;

namespace ProductDataIngestion.Repositories
{
    public class MCompanyRepository : IMCompanyRepository
    {

        private readonly string _connectionString;
        public MCompanyRepository(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task<MCompany?> FindBySourceDataAsync(string? SourceId)
        {
            const string sql = @"
                SELECT group_company_id FROM m_company
                WHERE 
                    is_active = TRUE 
                    AND group_company_cd = @SourceId;
            ";

            await using var connection = new NpgsqlConnection(_connectionString);
            return await connection.QueryFirstOrDefaultAsync<MCompany>(sql, new { SourceId = SourceId});
        }
    }
}
