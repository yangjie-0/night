using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ProductDataIngestion.Models;
using ProductDataIngestion.Repositories.Interfaces;

namespace ProductDataIngestion.Repositories
{
    /// <summary>
    /// m_cleanse_rule_set テーブルにアクセスするリポジトリクラス。
    /// クレンジングルールセットの取得・管理を行う。
    /// </summary>
    public class MCleanseRuleSetRepository : IMCleanseRuleSetRepository
    {
        private readonly string _connectionString;

        public MCleanseRuleSetRepository(string connectionString)
        {
            _connectionString = connectionString;
        }

        /// <summary>
        /// ルールセットIDで単一レコードを取得。
        /// </summary>
        public async Task<MCleanseRuleSet?> GetByIdAsync(long ruleSetId)
        {
            const string sql = @"
                SELECT *
                FROM m_cleanse_rule_set
                WHERE rule_set_id = @RuleSetId
                  AND is_active = TRUE;
            ";
            await using var connection = new NpgsqlConnection(_connectionString);
            return await connection.QueryFirstOrDefaultAsync<MCleanseRuleSet>(sql, new { RuleSetId = ruleSetId });
        }

        /// <summary>
        /// 有効な全ルールセットを取得。
        /// </summary>
        public async Task<IEnumerable<MCleanseRuleSet>> GetAllAsync()
        {
            const string sql = @"
                SELECT *
                FROM m_cleanse_rule_set
                WHERE is_active = TRUE
                ORDER BY released_at DESC;
            ";
            await using var connection = new NpgsqlConnection(_connectionString);
            return await connection.QueryAsync<MCleanseRuleSet>(sql);
        }
    }
}
