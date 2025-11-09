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
    public class MBrandGRepository : IMBrandGRepository
    {

        private readonly string _connectionString;
        public MBrandGRepository(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task<MBrandG?> GetByGBrandIdAsync(long id)
        {
            const string sql = @"
                SELECT g_brand_cd,g_brand_nm  
                FROM m_brand_g 
                WHERE g_brand_id = @Id
                AND is_active = TRUE;
            ";
            await using var connection = new NpgsqlConnection(_connectionString);
            return await connection.QueryFirstOrDefaultAsync<MBrandG>(sql, new { Id = id });
        }

        public async Task<long?> GetIdByCodeAsync(string brandCode, CancellationToken cancellationToken)
        {
            const string sql = @"
                SELECT g_brand_id
                FROM m_brand_g
                WHERE g_brand_cd = @BrandCode
                  AND is_active = TRUE
                LIMIT 1;
            ";

            await using var connection = new NpgsqlConnection(_connectionString);
            return await connection.ExecuteScalarAsync<long?>(
                new CommandDefinition(
                    sql,
                    new { BrandCode = brandCode },
                    cancellationToken: cancellationToken));
        }
    }
}
