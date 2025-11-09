using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ProductDataIngestion.Models;
using ProductDataIngestion.Repositories.Interfaces;

namespace ProductDataIngestion.Repositories
{
    public class BrandSourceMapRepository : IBrandSourceMapRepository
    {

        private readonly string _connectionString;
        public BrandSourceMapRepository(string connectionString)
        {
            _connectionString = connectionString;
        }


        public async Task<long?> FindBySourceDataAsync(string? sourceId)
        {
            const string sql = @"
                SELECT g_brand_id
                FROM brand_source_map
                WHERE 
                is_active = TRUE 
                AND source_brand_id = @SourceId;
            ";
            await using var connection = new NpgsqlConnection(_connectionString);
            return await connection.QueryFirstOrDefaultAsync<long?>(sql,
                            new { SourceId = sourceId });
        }
    }
}
