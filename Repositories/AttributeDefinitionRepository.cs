using Dapper;
using Npgsql;
using System.Collections.Generic;
using System.Threading.Tasks;
using ProductDataIngestion.Models;
using ProductDataIngestion.Repositories.Interfaces;

namespace ProductDataIngestion.Repositories
{
    public class AttributeDefinitionRepository : IAttributeDefinitionRepository
    {

        private readonly string _connectionString;
        public AttributeDefinitionRepository(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task<AttributeDefinition?> GetByCodeAsync(string attrCd)
        {
            const string sql = @"
                SELECT
                    attr_id,
                    attr_cd,
                    attr_nm,
                    attr_sort_no,
                    g_category_cd,
                    data_type,
                    g_list_group_cd,
                    select_type,
                    is_golden_attr,
                    cleanse_phase,
                    required_context_keys,
                    target_table,
                    target_column,
                    product_unit_cd,
                    credit_active_flag,
                    usage,
                    table_type_cd,
                    is_golden_product,
                    is_golden_eav AS IsGoldenAttrEav,
                    is_active,
                    attr_remarks,
                    cre_at,
                    upd_at
                FROM m_attr_definition
                WHERE attr_cd = @AttrCd AND is_active = TRUE;
            ";
            await using var connection = new NpgsqlConnection(_connectionString);
            return await connection.QueryFirstOrDefaultAsync<AttributeDefinition>(sql, new { AttrCd = attrCd });
        }

        // 缓存のために全件取得するメソッド
        public async Task<IEnumerable<AttributeDefinition>> GetAllAttrDefinitionAsync()
        {
            const string sql = @"
                SELECT
                    attr_id,
                    attr_cd,
                    attr_nm,
                    attr_sort_no,
                    g_category_cd,
                    data_type,
                    g_list_group_cd,
                    select_type,
                    is_golden_attr,
                    cleanse_phase,
                    required_context_keys,
                    target_table,
                    target_column,
                    product_unit_cd,
                    credit_active_flag,
                    usage,
                    table_type_cd,
                    is_golden_product,
                    is_golden_eav AS IsGoldenAttrEav,
                    is_active,
                    attr_remarks,
                    cre_at,
                    upd_at
                FROM m_attr_definition
                WHERE is_active = TRUE;
            ";

            await using var connection = new NpgsqlConnection(_connectionString);
            return await connection.QueryAsync<AttributeDefinition>(sql);
        }
    }
}
