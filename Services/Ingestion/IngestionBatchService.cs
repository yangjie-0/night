using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using ProductDataIngestion.Models;
using ProductDataIngestion.Repositories.Interfaces;

namespace ProductDataIngestion.Services.Ingestion
{
    /// <summary>
    /// batch_run テーブル周りの操作を集約したサービス。
    /// IngestService から切り離すことで責務分散を図る。
    /// </summary>
    public class IngestionBatchService
    {
        private readonly IBatchRepository _batchRepository;
        private readonly List<BatchRun> _batchRuns = new();

        public IngestionBatchService(IBatchRepository batchRepository)
        {
            _batchRepository = batchRepository ?? throw new ArgumentNullException(nameof(batchRepository));
        }

        /// <summary>
        /// バッチを新規作成し、内部キャッシュにも保持する。
        /// </summary>
        public async Task<string> CreateBatchAsync(string filePath, string groupCompanyCd, string targetEntity)
        {
            try
            {
                string batchId = $"BATCH_{DateTime.Now:yyyyMMddHHmmss}_{Guid.NewGuid():N}";
                var fileInfo = new FileInfo(filePath);
                string idemKey = $"{filePath}_{fileInfo.LastWriteTime.Ticks}";

                var batchRun = new BatchRun
                {
                    BatchId = batchId,
                    IdemKey = idemKey,
                    GroupCompanyCd = groupCompanyCd,
                    DataKind = targetEntity,
                    FileKey = filePath,
                    BatchStatus = "RUNNING",
                    StartedAt = DateTime.UtcNow,
                    CountsJson = "{\"INGEST\":{\"read\":0,\"ok\":0,\"ng\":0}}"
                };

                await _batchRepository.CreateBatchRunAsync(batchRun);
                _batchRuns.Add(batchRun);

                Console.WriteLine($"バッチ作成完了: {batchId}");
                return batchId;
            }
            catch (Exception ex)
            {
                throw new IngestException(
                    ErrorCodes.DB_ERROR,
                    $"バッチ作成に失敗しました: {ex.Message}",
                    ex
                );
            }
        }

        /// <summary>
        /// 読み込み件数などの集計値を更新する。
        /// </summary>
        public async Task UpdateStatisticsAsync(string batchId, (int readCount, int okCount, int ngCount) result)
        {
            try
            {
                var batchRun = _batchRuns.FirstOrDefault(b => b.BatchId == batchId);
                if (batchRun == null)
                {
                    Console.WriteLine($"バッチID {batchId} が内部キャッシュに存在しません。統計更新をスキップします。");
                    return;
                }

                batchRun.CountsJson = JsonSerializer.Serialize(new
                {
                    INGEST = new { read = result.readCount, ok = result.okCount, ng = result.ngCount },
                    CLEANSE = new { },
                    UPSERT = new { },
                    CATALOG = new { }
                });

                batchRun.BatchStatus = result.ngCount > 0 ? "PARTIAL" : "SUCCESS";
                batchRun.EndedAt = DateTime.UtcNow;

                await _batchRepository.UpdateBatchRunAsync(batchRun);
                Console.WriteLine($"バッチ統計を更新しました: status={batchRun.BatchStatus}");
            }
            catch (Exception ex)
            {
                throw new IngestException(
                    ErrorCodes.DB_ERROR,
                    $"バッチ統計の更新に失敗しました: {ex.Message}",
                    ex
                );
            }
        }

        /// <summary>
        /// バッチをFAILEDにマーキングする。
        /// </summary>
        public async Task MarkFailedAsync(string batchId, string errorMessage)
        {
            try
            {
                var batchRun = _batchRuns.FirstOrDefault(b => b.BatchId == batchId);
                if (batchRun == null)
                {
                    Console.WriteLine($"FAILED更新対象のバッチが見つかりませんでした: {batchId}");
                    return;
                }

                batchRun.BatchStatus = "FAILED";
                batchRun.EndedAt = DateTime.UtcNow;
                await _batchRepository.UpdateBatchRunAsync(batchRun);
                Console.WriteLine($"バッチをFAILEDに更新しました: {errorMessage}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"バッチFAILED更新でエラーが発生しました: {ex.Message}");
            }
        }

        /// <summary>
        /// キャッシュされたバッチ情報を取得する。主にテスト用途。
        /// </summary>
        public BatchRun? GetCachedBatch(string batchId) => _batchRuns.FirstOrDefault(b => b.BatchId == batchId);
    }
}
