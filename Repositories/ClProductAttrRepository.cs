using Dapper;
using Npgsql;
using System.Collections.Generic;
using System.Threading.Tasks;
using ProductDataIngestion.Models;
using ProductDataIngestion.Repositories.Interfaces;
using ProductDataIngestion.Utils;

namespace ProductDataIngestion.Repositories
{
    public class ClProductAttrRepository : IClProductAttrRepository
    {
        private readonly string _connectionString;

        public ClProductAttrRepository(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task<IEnumerable<ClProductAttr>> GetImportAttributesAsync(string batchId)
        {
            const string sql = @"
                SELECT 
                    batch_id,
                    temp_row_id,
                    attr_cd,
                    attr_seq,
                    source_id,
                    source_label,
                    source_raw
                FROM cl_product_attr
                WHERE 
                    batch_id = @BatchId
                    AND value_text IS NULL
                    AND value_num IS NULL
                    AND value_date IS NULL
                    AND value_cd IS NULL;
            ";

            await using var connection = new NpgsqlConnection(_connectionString);
            return await connection.QueryAsync<ClProductAttr>(sql, new { BatchId = batchId });
        }


        // クレンジングが完了した属性レコードをデータベースに更新します。
        public async Task UpdateProductAttrAsync(ClProductAttr entity)
        {
            // このSQL文は、クレンジング結果を格納するための主要な列をすべて更新します。
            const string sql = @"
                UPDATE cl_product_attr
                SET
                    value_cd = @ValueCd,
                    value_text = @ValueText,
                    value_num = @ValueNum,
                    value_date = @ValueDate,
                    quality_status = @QualityStatus,
                    quality_detail_json = CAST(@QualityDetailJson AS jsonb),
                    provenance_json = CAST(@ProvenanceJson AS jsonb),
                    rule_version = @RuleVersion,
                    upd_at = now()
                WHERE
                    batch_id = @BatchId
                    AND temp_row_id = @TempRowId
                    AND attr_cd = @AttrCd;
            ";

            await using var connection = new NpgsqlConnection(_connectionString);

            var affected = await connection.ExecuteAsync(sql, entity);
        }

        public async Task UpsertColorResultAsync(ClProductAttr entity)
        {
            const string sql = @"
        INSERT INTO cl_product_attr (
            batch_id,
            source_id,
            source_label,
            source_raw,
            temp_row_id,
            attr_cd,
            attr_seq,
            value_cd,
            value_text,
            data_type,
            quality_status,
            quality_detail_json,
            provenance_json,
            rule_version,
            cre_at,
            upd_at
        )
        VALUES (
            @BatchId,
            @SourceId,
            @SourceLabel,
            @SourceRaw,
            @TempRowId,
            @AttrCd,
            @AttrSeq,
            @ValueCd,
            @ValueText,
            @DataType,
            @QualityStatus,
            CAST(@QualityDetailJson AS jsonb),
            CAST(@ProvenanceJson AS jsonb),
            @RuleVersion,
            @CreAt,
            @UpdAt
        )
        ON CONFLICT (batch_id, temp_row_id,attr_cd, attr_seq)
        DO UPDATE SET
            value_cd = EXCLUDED.value_cd,
            value_text = EXCLUDED.value_text,
            quality_status = EXCLUDED.quality_status,
            quality_detail_json = EXCLUDED.quality_detail_json,
            provenance_json = EXCLUDED.provenance_json,
            rule_version = EXCLUDED.rule_version,
            upd_at = EXCLUDED.upd_at;
    ";

            await using var connection = new NpgsqlConnection(_connectionString);
            var affected = await connection.ExecuteAsync(sql, entity);

            Logger.Info($"UPSERT完了: batch_id={entity.BatchId}, attr_cd={entity.AttrCd}, attr_seq={entity.AttrSeq}, affected={affected}");
        }




        public async Task<IEnumerable<ClProductAttr>> CheckErrorAsync(string batchId)
        {
            const string sql = @"
        SELECT 
            batch_id,
            temp_row_id,
            attr_cd,
            source_raw,
            quality_detail_json
        FROM cl_product_attr
        WHERE batch_id = @BatchId
          AND quality_status = 'NG'
          AND attr_cd ='BRAND';
    ";

            await using var connection = new NpgsqlConnection(_connectionString);
            return await connection.QueryAsync<ClProductAttr>(sql, new { BatchId = batchId });
        }

    }
}
