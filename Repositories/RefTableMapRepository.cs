using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ProductDataIngestion.Models;
using ProductDataIngestion.Repositories.Interfaces;

namespace ProductDataIngestion.Repositories
{
    public class RefTableMapRepository : IRefTableMapRepository
    {

        private readonly string _connectionString;
        public RefTableMapRepository(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task<RefTableMap?> GetByCodeAsync(string attrCd)
        {
            const string sql = @"
                SELECT * FROM m_ref_table_map 
                WHERE attr_cd = @AttrCd AND is_active = TRUE;
            ";
            await using var connection = new NpgsqlConnection(_connectionString);
            return await connection.QueryFirstOrDefaultAsync<RefTableMap>(sql, new { AttrCd = attrCd });
        }

        // 缓存のために全件取得するメソッド
        public async Task<IEnumerable<RefTableMap>> GetAllAsync()
        {
            const string sql = @"
                SELECT *
                FROM m_ref_table_map
                WHERE is_active = TRUE;
            ";
            await using var connection = new NpgsqlConnection(_connectionString);
            return await connection.QueryAsync<RefTableMap>(sql);
        }
    }
}
