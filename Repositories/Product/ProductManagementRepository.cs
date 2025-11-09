using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Npgsql;
using ProductDataIngestion.Models;
using ProductDataIngestion.Repositories.Interfaces;

namespace ProductDataIngestion.Repositories
{
    public class ProductManagementRepository : IProductManagementRepository
    {
        private const string ProductManagementIdSequence = "m_product_management_g_product_management_id_seq";

        public async Task<long?> FindActiveProductManagementIdAsync(
            NpgsqlConnection connection,
            long groupCompanyId,
            string sourceProductManagementCd,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            try
            {
                const string sql = @"
                    SELECT g_product_management_id
                    FROM m_product_management
                    WHERE group_company_id = @GroupCompanyId
                    AND source_product_management_cd = @SourceProductManagementCd
                    AND is_active = TRUE
                    AND is_provisional = FALSE
                    ORDER BY g_product_management_id DESC
                    LIMIT 1;
                ";

                return await connection.ExecuteScalarAsync<long?>(
                    new CommandDefinition(
                        sql,
                        new { GroupCompanyId = groupCompanyId, SourceProductManagementCd = sourceProductManagementCd },
                        transaction,
                        cancellationToken: cancellationToken));
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
            {
                // テーブルが存在しない場合は null を返す（エラーとして扱わない）
                return null;
            }
        }

        public async Task<long> GetNextProductManagementIdAsync(
            NpgsqlConnection connection,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            try
            {
                var sequenceSql = $"SELECT nextval('{ProductManagementIdSequence}');";
                return await connection.ExecuteScalarAsync<long>(
                    new CommandDefinition(sequenceSql, transaction: transaction, cancellationToken: cancellationToken));
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable || ex.SqlState == PostgresErrorCodes.UndefinedObject)
            {
                try
                {
                    const string fallbackSql = "SELECT g_product_management_id FROM m_product_management ORDER BY g_product_management_id DESC LIMIT 1 FOR UPDATE;";
                    var lastValue = await connection.ExecuteScalarAsync<long?>(
                        new CommandDefinition(fallbackSql, transaction: transaction, cancellationToken: cancellationToken));
                    return (lastValue ?? 0) + 1;
                }
                catch (PostgresException fallbackEx) when (fallbackEx.SqlState == PostgresErrorCodes.UndefinedTable)
                {
                    // テーブルが存在しない場合は、エラーを上位に投げる
                    throw new InvalidOperationException("m_product_management テーブルが存在しません。先に createTable1105.sql を実行してテーブルを作成してください。", fallbackEx);
                }
            }
        }

        public async Task<MProductManagement?> GetProductManagementAsync(
            NpgsqlConnection connection,
            long gProductManagementId,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            const string sql = @"
                SELECT
                    g_product_management_id AS GProductManagementId,
                    group_company_id AS GroupCompanyId,
                    source_product_management_cd AS SourceProductManagementCd,
                    g_brand_id AS GBrandId,
                    g_category_id AS GCategoryId,
                    description_text AS DescriptionText,
                    is_provisional AS IsProvisional,
                    source_product_cd AS SourceProductCd,
                    provenance_json AS ProvenanceJson,
                    batch_id AS BatchId,
                    is_active AS IsActive,
                    cre_at AS CreAt,
                    upd_at AS UpdAt
                FROM m_product_management
                WHERE g_product_management_id = @GProductManagementId
                FOR UPDATE;
            ";

            return await connection.QueryFirstOrDefaultAsync<MProductManagement>(
                new CommandDefinition(sql, new { GProductManagementId = gProductManagementId }, transaction, cancellationToken: cancellationToken));
        }

        public Task InsertProductManagementAsync(
            NpgsqlConnection connection,
            MProductManagement entity,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            const string sql = @"
                INSERT INTO m_product_management (
                    g_product_management_id,
                    group_company_id,
                    source_product_management_cd,
                    g_brand_id,
                    g_category_id,
                    description_text,
                    is_provisional,
                    source_product_cd,
                    provenance_json,
                    batch_id,
                    is_active,
                    cre_at,
                    upd_at
                ) VALUES (
                    @GProductManagementId,
                    @GroupCompanyId,
                    @SourceProductManagementCd,
                    @GBrandId,
                    @GCategoryId,
                    @DescriptionText,
                    @IsProvisional,
                    @SourceProductCd,
                    @ProvenanceJson::jsonb,
                    @BatchId,
                    @IsActive,
                    NOW(),
                    NOW()
                );
            ";

            return connection.ExecuteAsync(
                new CommandDefinition(
                    sql,
                    new
                    {
                        entity.GProductManagementId,
                        entity.GroupCompanyId,
                        entity.SourceProductManagementCd,
                        entity.GBrandId,
                        entity.GCategoryId,
                        entity.DescriptionText,
                        entity.IsProvisional,
                        entity.SourceProductCd,
                        entity.ProvenanceJson,
                        entity.BatchId,
                        entity.IsActive
                    },
                    transaction,
                    cancellationToken: cancellationToken));
        }

        public Task UpdateProductManagementAsync(
            NpgsqlConnection connection,
            long gProductManagementId,
            IDictionary<string, object?> values,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            if (values == null || !values.Any())
            {
                return Task.CompletedTask;
            }

            var setters = new List<string>();
            var parameters = new DynamicParameters();
            parameters.Add("GProductManagementId", gProductManagementId);

            int index = 0;
            foreach (var kvp in values)
            {
                var column = kvp.Key;
                var paramName = $"p{index++}";

                if (column.Equals("provenance_json", StringComparison.OrdinalIgnoreCase))
                {
                    setters.Add($"{column} = provenance_json || @{paramName}::jsonb");
                }
                else
                {
                    setters.Add($"{column} = @{paramName}");
                }
                parameters.Add(paramName, kvp.Value);
            }

            setters.Add("upd_at = @UpdAt");
            parameters.Add("UpdAt", DateTime.UtcNow);

            var sql = $"UPDATE m_product_management SET {string.Join(", ", setters)} WHERE g_product_management_id = @GProductManagementId;";

            return connection.ExecuteAsync(
                new CommandDefinition(sql, parameters, transaction, cancellationToken: cancellationToken));
        }

        public async Task<Dictionary<(string attrCd, short attrSeq), MProductManagementEav>> GetProductManagementEavMapAsync(
            NpgsqlConnection connection,
            long gProductManagementId,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            const string sql = @"
                SELECT
                    g_product_management_id AS GProductManagementId,
                    attr_cd AS AttrCd,
                    attr_seq AS AttrSeq,
                    value_text AS ValueText,
                    value_num AS ValueNum,
                    value_date AS ValueDate,
                    value_cd AS ValueCd,
                    unit_cd AS UnitCd,
                    quality_status AS QualityStatus,
                    quality_detail_json AS QualityDetailJson,
                    provenance_json AS ProvenanceJson,
                    batch_id AS BatchId,
                    is_active AS IsActive,
                    cre_at AS CreAt,
                    upd_at AS UpdAt
                FROM m_product_management_eav
                WHERE g_product_management_id = @GProductManagementId
                FOR UPDATE;
            ";

            var result = await connection.QueryAsync<MProductManagementEav>(
                new CommandDefinition(sql, new { GProductManagementId = gProductManagementId }, transaction, cancellationToken: cancellationToken));

            return result.ToDictionary(x => (x.AttrCd, x.AttrSeq));
        }

        public Task InsertProductManagementEavAsync(
            NpgsqlConnection connection,
            MProductManagementEav entity,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            const string sql = @"
                INSERT INTO m_product_management_eav (
                    g_product_management_id,
                    attr_cd,
                    attr_seq,
                    value_text,
                    value_num,
                    value_date,
                    value_cd,
                    unit_cd,
                    quality_status,
                    quality_detail_json,
                    provenance_json,
                    batch_id,
                    is_active,
                    cre_at,
                    upd_at
                ) VALUES (
                    @GProductManagementId,
                    @AttrCd,
                    @AttrSeq,
                    @ValueText,
                    @ValueNum,
                    @ValueDate,
                    @ValueCd,
                    @UnitCd,
                    @QualityStatus,
                    @QualityDetailJson::jsonb,
                    @ProvenanceJson::jsonb,
                    @BatchId,
                    @IsActive,
                    NOW(),
                    NOW()
                ) ON CONFLICT (g_product_management_id, attr_cd, attr_seq) DO NOTHING;
            ";

            return connection.ExecuteAsync(
                new CommandDefinition(
                    sql,
                    new
                    {
                        entity.GProductManagementId,
                        entity.AttrCd,
                        entity.AttrSeq,
                        entity.ValueText,
                        entity.ValueNum,
                        entity.ValueDate,
                        entity.ValueCd,
                        entity.UnitCd,
                        entity.QualityStatus,
                        entity.QualityDetailJson,
                        entity.ProvenanceJson,
                        entity.BatchId,
                        entity.IsActive
                    },
                    transaction,
                    cancellationToken: cancellationToken));
        }

        public Task UpdateProductManagementEavAsync(
            NpgsqlConnection connection,
            long gProductManagementId,
            string attrCd,
            short attrSeq,
            IDictionary<string, object?> values,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            if (values == null || !values.Any())
            {
                return Task.CompletedTask;
            }

            var setters = new List<string>();
            var parameters = new DynamicParameters();
            parameters.Add("GProductManagementId", gProductManagementId);
            parameters.Add("AttrCd", attrCd);
            parameters.Add("AttrSeq", attrSeq);

            int index = 0;
            foreach (var kvp in values)
            {
                var column = kvp.Key;
                var paramName = $"p{index++}";

                if (column.Equals("provenance_json", StringComparison.OrdinalIgnoreCase))
                {
                    setters.Add($"{column} = provenance_json || @{paramName}::jsonb");
                }
                else
                {
                    setters.Add($"{column} = @{paramName}");
                }
                parameters.Add(paramName, kvp.Value);
            }

            setters.Add("upd_at = @UpdAt");
            parameters.Add("UpdAt", DateTime.UtcNow);

            var sql = $"UPDATE m_product_management_eav SET {string.Join(", ", setters)} WHERE g_product_management_id = @GProductManagementId AND attr_cd = @AttrCd AND attr_seq = @AttrSeq;";

            return connection.ExecuteAsync(
                new CommandDefinition(sql, parameters, transaction, cancellationToken: cancellationToken));
        }

        public Task MarkProductManagementEavInactiveAsync(
            NpgsqlConnection connection,
            long gProductManagementId,
            string attrCd,
            short attrSeq,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            const string sql = @"
                UPDATE m_product_management_eav
                SET is_active = FALSE,
                    upd_at = NOW()
                WHERE g_product_management_id = @GProductManagementId
                AND attr_cd = @AttrCd
                AND attr_seq = @AttrSeq;
            ";

            return connection.ExecuteAsync(
                new CommandDefinition(
                    sql,
                    new { GProductManagementId = gProductManagementId, AttrCd = attrCd, AttrSeq = attrSeq },
                    transaction,
                    cancellationToken: cancellationToken));
        }
    }
}
