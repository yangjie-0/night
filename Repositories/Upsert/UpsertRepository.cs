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
    public class UpsertRepository : IUpsertRepository
    {
        private const string ProductIdSequence = "m_product_g_product_id_seq";
        private const string ProductIdentSequence = "m_product_ident_ident_id_seq";

        private readonly string _connectionString;

        public UpsertRepository(string connectionString)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        }

        public async Task<NpgsqlConnection> OpenConnectionAsync(CancellationToken cancellationToken)
        {
            var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);
            return connection;
        }

        public async Task<BatchRun?> LockBatchRunAsync(NpgsqlConnection connection, string batchId, NpgsqlTransaction transaction, CancellationToken cancellationToken)
        {
            const string sql = @"
                        SELECT
                            batch_id         AS BatchId,
                            idem_key         AS IdemKey,
                            s3_bucket        AS S3Bucket,
                            etag             AS Etag,
                            group_company_cd AS GroupCompanyCd,
                            data_kind        AS DataKind,
                            file_key         AS FileKey,
                            batch_status     AS BatchStatus,
                            counts_json      AS CountsJson,
                            started_at       AS StartedAt,
                            ended_at         AS EndedAt,
                            cre_at           AS CreAt,
                            upd_at           AS UpdAt
                        FROM batch_run
                        WHERE batch_id = @BatchId
                        FOR UPDATE SKIP LOCKED;
                        ";

            return await connection.QueryFirstOrDefaultAsync<BatchRun>(
                new CommandDefinition(sql, new { BatchId = batchId }, transaction, cancellationToken: cancellationToken));
        }

        public Task InitializeBatchRunAsync(NpgsqlConnection connection, string batchId, string countsJson, NpgsqlTransaction transaction, CancellationToken cancellationToken)
        {
            const string sql = @"
                        UPDATE batch_run
                        SET
                            batch_status = 'RUNNING',
                            counts_json  = @CountsJson::jsonb,
                            started_at   = NOW(),
                            ended_at     = NULL,
                            upd_at       = NOW()
                        WHERE batch_id = @BatchId;
                        ";

            return connection.ExecuteAsync(
                new CommandDefinition(
                    sql,
                    new { BatchId = batchId, CountsJson = countsJson },
                    transaction,
                    cancellationToken: cancellationToken));
        }

        public async Task<IEnumerable<ClProductAttr>> FetchProductAttributesAsync(NpgsqlConnection connection, string batchId, CancellationToken cancellationToken)
        {
            const string sql = @"
                        SELECT
                            batch_id,
                            temp_row_id,
                            attr_cd,
                            attr_seq,
                            source_id,
                            source_label,
                            source_raw,
                            value_text,
                            value_num,
                            value_date,
                            value_cd,
                            g_list_item_id,
                            data_type,
                            quality_status,
                            quality_detail_json,
                            provenance_json,
                            rule_version,
                            cre_at,
                            upd_at
                        FROM cl_product_attr
                        WHERE batch_id = @BatchId
                        AND quality_status IN ('OK','WARN');
                        ";

            return await connection.QueryAsync<ClProductAttr>(
                new CommandDefinition(sql, new { BatchId = batchId }, cancellationToken: cancellationToken));
        }

        public async Task<IEnumerable<ClProductEvent>> FetchProductEventsAsync(NpgsqlConnection connection, string batchId, CancellationToken cancellationToken)
        {
            const string sql = @"
                        SELECT
                            id_event_id         AS IdEventId,
                            temp_row_event_id   AS TempRowEventId,
                            batch_id            AS BatchId,
                            idem_key            AS IdemKey,
                            stock_effect_cd     AS StockEffectCd,
                            signed_qty_num      AS SignedQtyNum,
                            reversal_idem_key   AS ReversalIdemKey,
                            group_company_id    AS GroupCompanyId,
                            g_product_id        AS GProductId,
                            store_id            AS StoreId,
                            new_used_kbn_cd     AS NewUsedKbnCd,
                            event_ts            AS EventTs,
                            event_kind_cd       AS EventKindCd,
                            qty_num             AS QtyNum,
                            quality_status      AS QualityStatus,
                            quality_detail_json AS QualityDetailJson,
                            provenance_json     AS ProvenanceJson,
                            rule_version        AS RuleVersion,
                            cleanse_status      AS CleanseStatus,
                            upsert_status       AS UpsertStatus,
                            cre_at              AS CreAt,
                            upd_at              AS UpdAt
                        FROM cl_product_event
                        WHERE batch_id = @BatchId
                        AND cleanse_status = 'CLEANSED'
                        AND upsert_status  = 'PENDING'
                        AND stock_effect_cd IN ('IN','OUT','ADJUST')
                        FOR UPDATE SKIP LOCKED;
                        ";

            return await connection.QueryAsync<ClProductEvent>(
                new CommandDefinition(sql, new { BatchId = batchId }, cancellationToken: cancellationToken));
        }

        public async Task<long?> FindActiveProductIdAsync(NpgsqlConnection connection, long groupCompanyId, string sourceProductCd, NpgsqlTransaction transaction, CancellationToken cancellationToken)
        {
            const string sql = @"
                        SELECT g_product_id
                        FROM m_product_ident
                        WHERE group_company_id = @GroupCompanyId
                        AND source_product_cd = @SourceProductCd
                        AND is_active = TRUE
                        ORDER BY ident_id DESC
                        LIMIT 1;
                        ";

            return await connection.ExecuteScalarAsync<long?>(
                new CommandDefinition(
                    sql,
                    new { GroupCompanyId = groupCompanyId, SourceProductCd = sourceProductCd },
                    transaction,
                    cancellationToken: cancellationToken));
        }

        public async Task<long> GetNextProductIdAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken)
        {
            return await GetNextValueAsync(
                connection,
                transaction,
                ProductIdSequence,
                "SELECT g_product_id FROM m_product ORDER BY g_product_id DESC LIMIT 1 FOR UPDATE;",
                cancellationToken);
        }

        public async Task<long> GetNextProductIdentIdAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken)
        {
            return await GetNextValueAsync(
                connection,
                transaction,
                ProductIdentSequence,
                "SELECT ident_id FROM m_product_ident ORDER BY ident_id DESC LIMIT 1 FOR UPDATE;",
                cancellationToken);
        }

        public async Task<bool> InsertProductIdentAsync(NpgsqlConnection connection, MProductIdent entity, NpgsqlTransaction transaction, CancellationToken cancellationToken)
        {
            const string sql = @"
                INSERT INTO m_product_ident (
                    ident_id,
                    g_product_id,
                    group_company_id,
                    source_product_cd,
                    source_product_management_cd,
                    ident_kind,
                    confidence,
                    is_primary,
                    is_active,
                    valid_from,
                    valid_to,
                    provenance_json,
                    ident_remarks,
                    batch_id,
                    cre_at,
                    upd_at
                ) VALUES (
                    @IdentId,
                    @GProductId,
                    @GroupCompanyId,
                    @SourceProductCd,
                    @SourceProductManagementCd,
                    @IdentKind,
                    @Confidence,
                    @IsPrimary,
                    @IsActive,
                    @ValidFrom,
                    @ValidTo,
                    @ProvenanceJson::jsonb,
                    @IdentRemarks,
                    @BatchId,
                    NOW(),
                    NOW()
                )
                ON CONFLICT (group_company_id, source_product_cd) WHERE is_active = TRUE 
                DO NOTHING;
            ";

            var affected = await connection.ExecuteAsync(
                new CommandDefinition(
                    sql,
                    new
                    {
                        entity.IdentId,
                        entity.GProductId,
                        entity.GroupCompanyId,
                        entity.SourceProductCd,
                        entity.SourceProductManagementCd,
                        entity.IdentKind,
                        entity.Confidence,
                        entity.IsPrimary,
                        entity.IsActive,
                        entity.ValidFrom,
                        entity.ValidTo,
                        entity.ProvenanceJson,
                        entity.IdentRemarks,
                        entity.BatchId
                    },
                    transaction,
                    cancellationToken: cancellationToken));

            return affected > 0;
        }


        public async Task<MProduct?> GetProductAsync(NpgsqlConnection connection, long gProductId, NpgsqlTransaction transaction, CancellationToken cancellationToken)
        {
            const string sql = @"
                        SELECT
                            g_product_id              AS GProductId,
                            g_product_cd              AS GProductCd,
                            unit_no                   AS UnitNo,
                            group_company_id          AS GroupCompanyId,
                            source_product_cd         AS SourceProductCd,
                            source_product_management_cd AS SourceProductManagementCd,
                            g_brand_id                AS GBrandId,
                            g_category_id             AS GCategoryId,
                            currency_cd               AS CurrencyCd,
                            display_price_incl_tax    AS DisplayPriceInclTax,
                            product_status_cd         AS ProductStatusCd,
                            new_used_kbn_cd           AS NewUsedKbnCd,
                            stock_existence_cd        AS StockExistenceCd,
                            sale_status_cd            AS SaleStatusCd,
                            last_event_ts             AS LastEventTs,
                            last_event_kind_cd        AS LastEventKindCd,
                            is_active                 AS IsActive,
                            cre_at                    AS CreAt,
                            upd_at                    AS UpdAt
                        FROM m_product
                        WHERE g_product_id = @GProductId
                        FOR UPDATE;
                        ";

            return await connection.QueryFirstOrDefaultAsync<MProduct>(
                new CommandDefinition(sql, new { GProductId = gProductId }, transaction, cancellationToken: cancellationToken));
        }

        public Task InsertProductAsync(NpgsqlConnection connection, MProduct entity, NpgsqlTransaction transaction, CancellationToken cancellationToken)
        {
            const string sql = @"
                        INSERT INTO m_product (
                            g_product_id,
                            g_product_cd,
                            unit_no,
                            group_company_id,
                            source_product_cd,
                            source_product_management_cd,
                            g_brand_id,
                            g_category_id,
                            currency_cd,
                            display_price_incl_tax,
                            product_status_cd,
                            new_used_kbn_cd,
                            stock_existence_cd,
                            sale_status_cd,
                            last_event_ts,
                            last_event_kind_cd,
                            is_active,
                            cre_at,
                            upd_at
                        ) VALUES (
                            @GProductId,
                            @GProductCd,
                            @UnitNo,
                            @GroupCompanyId,
                            @SourceProductCd,
                            @SourceProductManagementCd,
                            @GBrandId,
                            @GCategoryId,
                            @CurrencyCd,
                            @DisplayPriceInclTax,
                            @ProductStatusCd,
                            @NewUsedKbnCd,
                            @StockExistenceCd,
                            @SaleStatusCd,
                            @LastEventTs,
                            @LastEventKindCd,
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
                        entity.GProductId,
                        entity.GProductCd,
                        entity.UnitNo,
                        entity.GroupCompanyId,
                        entity.SourceProductCd,
                        entity.SourceProductManagementCd,
                        entity.GBrandId,
                        entity.GCategoryId,
                        entity.CurrencyCd,
                        entity.DisplayPriceInclTax,
                        entity.ProductStatusCd,
                        entity.NewUsedKbnCd,
                        entity.StockExistenceCd,
                        entity.SaleStatusCd,
                        entity.LastEventTs,
                        entity.LastEventKindCd,
                        entity.IsActive
                    },
                    transaction,
                    cancellationToken: cancellationToken));
        }

        public Task UpdateProductAsync(NpgsqlConnection connection, long gProductId, IDictionary<string, object?> values, NpgsqlTransaction transaction, CancellationToken cancellationToken)
        {
            if (values == null || !values.Any())
            {
                return Task.CompletedTask;
            }

            var setters = new List<string>();
            var parameters = new DynamicParameters();
            parameters.Add("GProductId", gProductId);

            int index = 0;
            foreach (var kvp in values)
            {
                var column = kvp.Key;
                var paramName = $"p{index++}";
                setters.Add($"{column} = @{paramName}");
                parameters.Add(paramName, kvp.Value);
            }

            setters.Add("upd_at = @UpdAt");
            parameters.Add("UpdAt", DateTime.UtcNow);

            var sql = $"UPDATE m_product SET {string.Join(", ", setters)} WHERE g_product_id = @GProductId;";

            return connection.ExecuteAsync(
                new CommandDefinition(sql, parameters, transaction, cancellationToken: cancellationToken));
        }

        public async Task<Dictionary<(string attrCd, short attrSeq), MProductEav>> GetProductEavMapAsync(NpgsqlConnection connection, long gProductId, NpgsqlTransaction transaction, CancellationToken cancellationToken)
        {
            const string sql = @"
                        SELECT
                            g_product_id        AS GProductId,
                            attr_cd             AS AttrCd,
                            attr_seq            AS AttrSeq,
                            value_text          AS ValueText,
                            value_num           AS ValueNum,
                            value_date          AS ValueDate,
                            value_cd            AS ValueCd,
                            unit_cd             AS UnitCd,
                            quality_status      AS QualityStatus,
                            quality_detail_json AS QualityDetailJson,
                            provenance_json     AS ProvenanceJson,
                            batch_id            AS BatchId,
                            is_active           AS IsActive,
                            cre_at              AS CreAt,
                            upd_at              AS UpdAt
                        FROM m_product_eav
                        WHERE g_product_id = @GProductId
                        FOR UPDATE;
                        ";

            var result = await connection.QueryAsync<MProductEav>(
                new CommandDefinition(sql, new { GProductId = gProductId }, transaction, cancellationToken: cancellationToken));

            return result.ToDictionary(x => (x.AttrCd, x.AttrSeq));
        }

        public Task InsertProductEavAsync(NpgsqlConnection connection, MProductEav entity, NpgsqlTransaction transaction, CancellationToken cancellationToken)
        {
            const string sql = @"
                        INSERT INTO m_product_eav (
                            g_product_id,
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
                            @GProductId,
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
                        ) ON CONFLICT (g_product_id, attr_cd, attr_seq) DO NOTHING;
                        ";

            return connection.ExecuteAsync(
                new CommandDefinition(
                    sql,
                    new
                    {
                        entity.GProductId,
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

        public Task UpdateProductEavAsync(NpgsqlConnection connection, long gProductId, string attrCd, short attrSeq, IDictionary<string, object?> values, NpgsqlTransaction transaction, CancellationToken cancellationToken)
        {
            if (values == null || !values.Any())
            {
                return Task.CompletedTask;
            }

            var setters = new List<string>();
            var parameters = new DynamicParameters();
            parameters.Add("GProductId", gProductId);
            parameters.Add("AttrCd", attrCd);
            parameters.Add("AttrSeq", attrSeq);

            int index = 0;
            foreach (var kvp in values)
            {
                var column = kvp.Key;
                var paramName = $"p{index++}";
                setters.Add($"{column} = @{paramName}");
                parameters.Add(paramName, kvp.Value);
            }

            setters.Add("upd_at = @UpdAt");
            parameters.Add("UpdAt", DateTime.UtcNow);

            var sql = $"UPDATE m_product_eav SET {string.Join(", ", setters)} WHERE g_product_id = @GProductId AND attr_cd = @AttrCd AND attr_seq = @AttrSeq;";

            return connection.ExecuteAsync(
                new CommandDefinition(sql, parameters, transaction, cancellationToken: cancellationToken));
        }

        public Task MarkProductEavInactiveAsync(NpgsqlConnection connection, long gProductId, string attrCd, short attrSeq, NpgsqlTransaction transaction, CancellationToken cancellationToken)
        {
            const string sql = @"
                        UPDATE m_product_eav
                        SET is_active = FALSE,
                            upd_at    = NOW()
                        WHERE g_product_id = @GProductId
                        AND attr_cd      = @AttrCd
                        AND attr_seq     = @AttrSeq;
                        ";

            return connection.ExecuteAsync(
                new CommandDefinition(
                    sql,
                    new { GProductId = gProductId, AttrCd = attrCd, AttrSeq = attrSeq },
                    transaction,
                    cancellationToken: cancellationToken));
        }

        public Task UpdateEventStatusAsync(NpgsqlConnection connection, Guid tempRowEventId, string status, NpgsqlTransaction transaction, CancellationToken cancellationToken)
        {
            const string sql = @"
                        UPDATE cl_product_event
                        SET upsert_status = @Status,
                            upd_at        = NOW()
                        WHERE temp_row_event_id = @TempRowEventId;
                        ";

            return connection.ExecuteAsync(
                new CommandDefinition(
                    sql,
                    new { TempRowEventId = tempRowEventId, Status = status },
                    transaction,
                    cancellationToken: cancellationToken));
        }

        public Task UpdateBatchRunCompletionAsync(NpgsqlConnection connection, string batchId, string countsJson, string status, NpgsqlTransaction transaction, CancellationToken cancellationToken)
        {
            const string sql = @"
                        UPDATE batch_run
                        SET
                            batch_status = @Status,
                            counts_json  = @CountsJson::jsonb,
                            ended_at     = NOW(),
                            upd_at       = NOW()
                        WHERE batch_id = @BatchId;
                        ";

            return connection.ExecuteAsync(
                new CommandDefinition(
                    sql,
                    new { BatchId = batchId, CountsJson = countsJson, Status = status },
                    transaction,
                    cancellationToken: cancellationToken));
        }

        private static async Task<long> GetNextValueAsync(
            NpgsqlConnection connection,
            NpgsqlTransaction transaction,
            string sequenceName,
            string fallbackSql,
            CancellationToken cancellationToken)
        {
            try
            {
                var sequenceSql = $"SELECT nextval('{sequenceName}');";
                return await connection.ExecuteScalarAsync<long>(
                    new CommandDefinition(sequenceSql, transaction: transaction, cancellationToken: cancellationToken));
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable || ex.SqlState == PostgresErrorCodes.UndefinedObject)
            {
                var lastValue = await connection.ExecuteScalarAsync<long?>(
                    new CommandDefinition(fallbackSql, transaction: transaction, cancellationToken: cancellationToken));
                return (lastValue ?? 0) + 1;
            }
        }
    }
}
