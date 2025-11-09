using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ProductDataIngestion.Models;
using ProductDataIngestion.Repositories.Interfaces;

namespace ProductDataIngestion.Repositories
{
    public class MListItemGRepository : IMListItemGRepository
    {

        private readonly string _connectionString;
        public MListItemGRepository(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task<MListItemG?> GetByListItemIdAsync(long id)
        {
            const string sql = @"
                SELECT g_item_cd,g_item_label 
                FROM m_list_item_g 
                WHERE g_list_item_id = @Id AND is_active = TRUE;
            ";
            await using var connection = new NpgsqlConnection(_connectionString);
            return await connection.QueryFirstOrDefaultAsync<MListItemG>(sql, new { Id = id });
        }

        public async Task<IEnumerable<MListItemG>> GetAllAsync()
        {
            const string sql = @"
                SELECT g_item_cd, g_item_label
                FROM m_list_item_g
                WHERE is_active = TRUE;
            ";
            await using var conn = new NpgsqlConnection(_connectionString);
            return await conn.QueryAsync<MListItemG>(sql);
        }
    }
}
