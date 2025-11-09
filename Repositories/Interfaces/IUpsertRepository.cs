using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using ProductDataIngestion.Models;

namespace ProductDataIngestion.Repositories.Interfaces
{
    /// <summary>
    /// UPSERT 処理で利用するDBアクセスのインターフェース。
    /// トランザクションは呼び出し側(サービス層)が開始・コミット/ロールバックを管理します。
    /// </summary>
    public interface IUpsertRepository
    {
        /// <summary>
        /// DB接続を開く。
        /// </summary>
        Task<NpgsqlConnection> OpenConnectionAsync(CancellationToken cancellationToken);

        /// <summary>
        /// 対象バッチ行を取得し、FOR UPDATE SKIP LOCKED でロックする(二重実行を回避)。
        /// </summary>
        Task<BatchRun?> LockBatchRunAsync(NpgsqlConnection connection, string batchId, NpgsqlTransaction transaction, CancellationToken cancellationToken);

        /// <summary>
        /// バッチ行を RUNNING に更新し、counts_json をUPSERT用に初期化する。
        /// started_at/ended_at も適切に更新。
        /// </summary>
        Task InitializeBatchRunAsync(NpgsqlConnection connection, string batchId, string countsJson, NpgsqlTransaction transaction, CancellationToken cancellationToken);

        /// <summary>
        /// PRODUCT 用: クレンジング済み属性(cl_product_attr)を取得する(OK/WARN)。
        /// </summary>
        Task<IEnumerable<ClProductAttr>> FetchProductAttributesAsync(NpgsqlConnection connection, string batchId, CancellationToken cancellationToken);

        /// <summary>
        /// EVENT 用: クレンジング済み・未反映のイベント(cl_product_event)を取得する。
        /// 行ロックで SKIP LOCKED。
        /// </summary>
        Task<IEnumerable<ClProductEvent>> FetchProductEventsAsync(NpgsqlConnection connection, string batchId, CancellationToken cancellationToken);

        /// <summary>
        /// 既存のアクティブな m_product_ident から g_product_id を検索。
        /// </summary>
        Task<long?> FindActiveProductIdAsync(NpgsqlConnection connection, long groupCompanyId, string sourceProductCd, NpgsqlTransaction transaction, CancellationToken cancellationToken);

        /// <summary>
        /// m_product の採番値を取得(シーケンス/フォールバック)。
        /// </summary>
        Task<long> GetNextProductIdAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken);

        /// <summary>
        /// m_product_ident の採番値を取得(シーケンス/フォールバック)。
        /// </summary>
        Task<long> GetNextProductIdentIdAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken);

        /// <summary>
        /// m_product_ident へ新規登録。
        /// </summary>
        Task<bool> InsertProductIdentAsync(NpgsqlConnection connection, MProductIdent entity, NpgsqlTransaction transaction, CancellationToken cancellationToken);

        /// <summary>
        /// m_product から対象行をロック取得(FOR UPDATE)。
        /// </summary>
        Task<MProduct?> GetProductAsync(NpgsqlConnection connection, long gProductId, NpgsqlTransaction transaction, CancellationToken cancellationToken);

        /// <summary>
        /// m_product へ新規登録。
        /// </summary>
        Task InsertProductAsync(NpgsqlConnection connection, MProduct entity, NpgsqlTransaction transaction, CancellationToken cancellationToken);

        /// <summary>
        /// m_product の差分UPDATE(指定カラムのみ更新)。
        /// </summary>
        Task UpdateProductAsync(NpgsqlConnection connection, long gProductId, IDictionary<string, object?> values, NpgsqlTransaction transaction, CancellationToken cancellationToken);

        /// <summary>
        /// 既存の m_product_eav をロック取得し、(attr_cd, attr_seq)→行 のマップを返す。
        /// </summary>
        Task<Dictionary<(string attrCd, short attrSeq), MProductEav>> GetProductEavMapAsync(NpgsqlConnection connection, long gProductId, NpgsqlTransaction transaction, CancellationToken cancellationToken);

        /// <summary>
        /// m_product_eav へ新規登録(存在衝突時は何もしない)。
        /// </summary>
        Task InsertProductEavAsync(NpgsqlConnection connection, MProductEav entity, NpgsqlTransaction transaction, CancellationToken cancellationToken);

        /// <summary>
        /// m_product_eav の差分UPDATE(指定カラムのみ更新)。
        /// </summary>
        Task UpdateProductEavAsync(NpgsqlConnection connection, long gProductId, string attrCd, short attrSeq, IDictionary<string, object?> values, NpgsqlTransaction transaction, CancellationToken cancellationToken);

        /// <summary>
        /// 出現しなかったEAV属性を非アクティブ化(is_active=false)。
        /// </summary>
        Task MarkProductEavInactiveAsync(NpgsqlConnection connection, long gProductId, string attrCd, short attrSeq, NpgsqlTransaction transaction, CancellationToken cancellationToken);

        /// <summary>
        /// イベント反映ステータスの更新。
        /// </summary>
        Task UpdateEventStatusAsync(NpgsqlConnection connection, Guid tempRowEventId, string status, NpgsqlTransaction transaction, CancellationToken cancellationToken);

        /// <summary>
        /// バッチの完了更新(ステータス/ended_at/counts_json)。
        /// </summary>
        Task UpdateBatchRunCompletionAsync(NpgsqlConnection connection, string batchId, string countsJson, string status, NpgsqlTransaction transaction, CancellationToken cancellationToken);
    }
}
