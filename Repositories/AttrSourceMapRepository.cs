using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ProductDataIngestion.Models;
using ProductDataIngestion.Repositories.Interfaces;

namespace ProductDataIngestion.Repositories
{
    public class AttrSourceMapRepository : IAttrSourceMapRepository
    {

        private readonly string _connectionString;
        public AttrSourceMapRepository(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task<long?> FindBySourceDataAsync(string? sourceId, string? sourceName)
        {
            const string sql = @"
                SELECT g_list_item_id 
                FROM attr_source_map
                WHERE 
                    is_active = TRUE 
                    AND source_attr_id = @SourceId 
                    AND source_attr_nm = @SourceName;
            ";
            await using var connection = new NpgsqlConnection(_connectionString);
            
            return await connection.QueryFirstOrDefaultAsync<long>(sql, 
                new { SourceId = sourceId, SourceName = sourceName });
        }
    }
}
