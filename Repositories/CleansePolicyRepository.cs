using Dapper;
using Npgsql;
using System.Collections.Generic;
using System.Threading.Tasks;
using ProductDataIngestion.Models;
using ProductDataIngestion.Repositories.Interfaces;
using ProductDataIngestion.Utils;

namespace ProductDataIngestion.Repositories
{
    public class CleansePolicyRepository : ICleansePolicyRepository
    {

        private readonly string _connectionString;
        public CleansePolicyRepository(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task<CleansePolicy?> GetByCodeAsync(string attrCd)
        {
            const string sql = @"
                SELECT data_type,step_no,matcher_kind,derive_from_attr_cds
                FROM m_attr_cleanse_policy 
                WHERE attr_cd = @AttrCd AND is_active = TRUE;
            ";
            await using var connection = new NpgsqlConnection(_connectionString);
            return await connection.QueryFirstOrDefaultAsync<CleansePolicy>(sql, new { AttrCd = attrCd });
        }

        public async Task<IEnumerable<CleansePolicy>> GetAllAsync()
        {
            const string sql = "SELECT * FROM m_attr_cleanse_policy WHERE is_active = TRUE;";
            await using var connection = new NpgsqlConnection(_connectionString);
            return await connection.QueryAsync<CleansePolicy>(sql);
        }

        public async Task<CleansePolicy?> GetPolicyAsync(string attrCd, string? groupCompanyCd, string? brand, string? GCategoryCd)
        {
            // 基础 SQL（始终按 attr_cd）
            var sql = @"
                SELECT *
                FROM m_attr_cleanse_policy
                WHERE attr_cd = @AttrCd
            ";

            // 可选条件：brand_scope / gp_scope 
            // ※category_scope は未実装
            if (!string.IsNullOrWhiteSpace(groupCompanyCd))
                sql += " AND gp_scope = @GroupCompanyCd";

            Logger.Info($"※※※groupCompanyCd: {groupCompanyCd}");

            if (!string.IsNullOrWhiteSpace(brand))
                sql += " AND brand_scope = @Brand";

            Logger.Info($"※※※Brand: {brand}");

            if (!string.IsNullOrWhiteSpace(GCategoryCd))
                sql += " AND category_scope = @GCategoryCd";

            Logger.Info($"GCategoryCd: {GCategoryCd}");

            // 优先精确范围匹配，没匹配到时取第一条
            sql += @"
                ORDER BY COALESCE(brand_scope, '') DESC,
                        COALESCE(gp_scope, '') DESC
                LIMIT 1;
            ";

            await using var conn = new NpgsqlConnection(_connectionString);

            var result = await conn.QueryFirstOrDefaultAsync<CleansePolicy>(
                sql,
                new
                {
                    AttrCd = attrCd,
                    GroupCompanyCd = groupCompanyCd,
                    Brand = brand
                });

            return result;
        }

        // CleansePolicyRepository.cs
        public async Task<IEnumerable<CleansePolicy>> GetPoliciesAsync(string attrCd, string? groupCompanyCd)
        {
            var sql = @"
                SELECT *
                FROM m_attr_cleanse_policy
                WHERE attr_cd = @AttrCd
                AND (gp_scope IS NULL OR gp_scope = @GroupCompanyCd)
                ORDER BY step_no ASC;
            ";

            await using var conn = new NpgsqlConnection(_connectionString);
            return await conn.QueryAsync<CleansePolicy>(sql, new
            {
                AttrCd = attrCd,
                GroupCompanyCd = groupCompanyCd
            });
        }


    }
}
