using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ProductDataIngestion.Models;
using ProductDataIngestion.Repositories.Interfaces;

namespace ProductDataIngestion.Repositories
{
    public class CategorySourceMapRepository : ICategorySourceMapRepository
    {

        private readonly string _connectionString;
        public CategorySourceMapRepository(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task<long?> FindByCategoryAsync(string attrCd, string? sourceId)
        {
            string sql;

            // attrCdの値に基づいて、実行するSQL文を動的に選択します。
            switch (attrCd)
            {
                case "CATEGORY_1":
                    sql = @"SELECT g_category_id FROM category_source_map
                            WHERE is_active = TRUE
                            AND (source_category_1_id = @SourceId);";
                    break;

                case "CATEGORY_2":
                    sql = @"SELECT g_category_id FROM category_source_map
                            WHERE is_active = TRUE
                            AND (source_category_2_id = @SourceId);";
                    break;

                case "CATEGORY_3":
                    sql = @"SELECT g_category_id FROM category_source_map
                            WHERE is_active = TRUE
                            AND (source_category_3_id = @SourceId);";
                    break;

                default:
                    return null;
            }

            await using var connection = new NpgsqlConnection(_connectionString);
            return await connection.QueryFirstOrDefaultAsync<long>(sql,
                new { SourceId = sourceId});
        }
    }
}
