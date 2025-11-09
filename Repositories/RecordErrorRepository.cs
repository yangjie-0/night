using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ProductDataIngestion.Models;
using ProductDataIngestion.Repositories.Interfaces;

namespace ProductDataIngestion.Repositories
{
    public class RecordErrorRepository : IRecordErrorRepository
    {
        private readonly string _connectionString;

        public RecordErrorRepository(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task InsertAsync(RecordError error)
        {
            if (error == null) throw new ArgumentNullException(nameof(error));

            // Ensure ErrorId and timestamps
            if (error.ErrorId == Guid.Empty)
                error.ErrorId = Guid.NewGuid();

            if (error.CreAt == default) error.CreAt = DateTime.UtcNow;
            error.UpdAt = DateTime.UtcNow;

            const string sql = @"
                INSERT INTO record_error (
                    error_id, batch_id, step, record_ref, error_cd, error_detail, raw_fragment, cre_at, upd_at
                ) VALUES (
                    @ErrorId, @BatchId, @Step, @RecordRef, @ErrorCd, @ErrorDetail, @RawFragment, @CreAt, @UpdAt
                ) ON CONFLICT (error_id) DO NOTHING;
            ";

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.ExecuteAsync(sql, new
            {
                error.ErrorId,
                error.BatchId,
                error.Step,
                RecordRef = error.RecordRef,
                ErrorCd = error.ErrorCd,
                ErrorDetail = error.ErrorDetail,
                RawFragment = error.RawFragment,
                CreAt = error.CreAt,
                UpdAt = error.UpdAt
            });
        }

        public async Task<IEnumerable<RecordError>> GetByBatchIdAsync(string batchId)
        {
            const string sql = @"
                SELECT error_id AS ErrorId,
                       batch_id AS BatchId,
                       step AS Step,
                       record_ref AS RecordRef,
                       error_cd AS ErrorCd,
                       error_detail AS ErrorDetail,
                       raw_fragment AS RawFragment,
                       cre_at AS CreAt,
                       upd_at AS UpdAt
                FROM record_error
                WHERE batch_id = @BatchId
                ORDER BY cre_at DESC;
            ";

            await using var conn = new NpgsqlConnection(_connectionString);
            return await conn.QueryAsync<RecordError>(sql, new { BatchId = batchId });
        }
    }
}
