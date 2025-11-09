using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ProductDataIngestion.Models;
using ProductDataIngestion.Repositories.Interfaces;

namespace ProductDataIngestion.Repositories
{
    public class MCategoryGRepository : IMCategoryGRepository
    {
        private readonly string _connectionString;
        public MCategoryGRepository(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task<MCategoryG?> GetByIdAsync(long id)
        {
            const string sql = @"
                SELECT g_category_cd,g_category_nm
                FROM m_category_g 
                WHERE g_category_id = @Id AND is_active = TRUE;
            ";
            await using var connection = new NpgsqlConnection(_connectionString);
            return await connection.QueryFirstOrDefaultAsync<MCategoryG>(sql, new { Id = id });
        }

        public async Task<long?> GetIdByCodeAsync(string categoryCode, CancellationToken cancellationToken)
        {
            const string sql = @"
                SELECT g_category_id
                FROM m_category_g
                WHERE g_category_cd = @CategoryCode
                  AND is_active = TRUE
                LIMIT 1;
            ";

            await using var connection = new NpgsqlConnection(_connectionString);
            return await connection.ExecuteScalarAsync<long?>(
                new CommandDefinition(
                    sql,
                    new { CategoryCode = categoryCode },
                    cancellationToken: cancellationToken));
        }
    }
}
