using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Npgsql;
using ProductDataIngestion.Models;
using ProductDataIngestion.Repositories.Interfaces;
using ProductDataIngestion.Services.Upsert;
using ProductDataIngestion.Utils;

namespace ProductDataIngestion.Services
{
    /// <summary>
    /// バッチ単位でPRODUCT系データのUPSERTを実行するサービス。
    /// 主な流れ: バッチロック→参照データ解決→PRODUCT/EVENT分岐→商品単位UPSERT→counts更新。
    /// </summary>
    public class UpsertService
    {
        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            WriteIndented = false,
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };

        private readonly IUpsertRepository _upsertRepository;
        private readonly IRecordErrorRepository _recordErrorRepository;
        private readonly ICompanyRepository _companyRepository;
        private readonly IAttributeDefinitionRepository _attributeDefinitionRepository;
        private readonly IMCategoryGRepository _categoryRepository;
        private readonly IMBrandGRepository _brandRepository;
        private readonly IProductManagementRepository _productManagementRepository;
        private readonly UpsertCounters _counters = new();
        private readonly Dictionary<string, long> _companyIdCache = new(StringComparer.OrdinalIgnoreCase);

        private string? _baseCountsJson;

        public UpsertService(
            IUpsertRepository upsertRepository,
            IRecordErrorRepository recordErrorRepository,
            ICompanyRepository companyRepository,
            IAttributeDefinitionRepository attributeDefinitionRepository,
            IMCategoryGRepository categoryRepository,
            IMBrandGRepository brandRepository,
            IProductManagementRepository productManagementRepository)
        {
            _upsertRepository = upsertRepository ?? throw new ArgumentNullException(nameof(upsertRepository));
            _recordErrorRepository = recordErrorRepository ?? throw new ArgumentNullException(nameof(recordErrorRepository));
            _companyRepository = companyRepository ?? throw new ArgumentNullException(nameof(companyRepository));
            _attributeDefinitionRepository = attributeDefinitionRepository ?? throw new ArgumentNullException(nameof(attributeDefinitionRepository));
            _categoryRepository = categoryRepository ?? throw new ArgumentNullException(nameof(categoryRepository));
            _brandRepository = brandRepository ?? throw new ArgumentNullException(nameof(brandRepository));
            _productManagementRepository = productManagementRepository ?? throw new ArgumentNullException(nameof(productManagementRepository));
        }
        /// <summary>
        /// 指定したバッチIDのUPSERT処理を実行する。
        /// </summary>
        public async Task ExecuteAsync(string batchId, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(batchId))
            {
                throw new ArgumentException("バッチIDが指定されていません。", nameof(batchId));
            }

            _counters.Reset();

            await using var connection = await _upsertRepository.OpenConnectionAsync(cancellationToken);
            BatchRun batchRun;

            await using (var tx = await connection.BeginTransactionAsync(cancellationToken))
            {
                Logger.Info($"UPSERT開始: batch_id={batchId}");
                batchRun = await _upsertRepository
                    .LockBatchRunAsync(connection, batchId, tx, cancellationToken)
                    ?? throw new InvalidOperationException("指定されたバッチは他プロセスで処理中です。");

                _baseCountsJson = batchRun.CountsJson;
                var initializedCounts = BuildCountsJsonSnapshot();
                await _upsertRepository.InitializeBatchRunAsync(connection, batchId, initializedCounts, tx, cancellationToken);
                await tx.CommitAsync(cancellationToken);

                _baseCountsJson = initializedCounts;
                Logger.Info($"バッチ初期化完了: batch_id={batchId}, data_kind={batchRun.DataKind}, group_company_cd={batchRun.GroupCompanyCd}");
            }

            var definitionMap = await LoadDefinitionMapAsync(cancellationToken);
            var defaultCompany = await _companyRepository.GetActiveCompanyAsync(batchRun.GroupCompanyCd)
                ?? throw new InvalidOperationException($"GP会社コード {batchRun.GroupCompanyCd} に対応する会社が見つかりません。");

            var defaultGroupCompanyId = ParseGroupCompanyId(defaultCompany);
            _companyIdCache[batchRun.GroupCompanyCd] = defaultGroupCompanyId;
            Logger.Info($"参照解決: 属性定義件数={definitionMap.Count}, group_company_id={defaultGroupCompanyId}");

            if (string.Equals(batchRun.DataKind, "EVENT", StringComparison.OrdinalIgnoreCase))
            {
                await ProcessEventsAsync(connection, batchRun, cancellationToken);
            }
            else
            {
                await ProcessProductsAsync(connection, batchRun, defaultGroupCompanyId, definitionMap, cancellationToken);
            }

            var finalStatus = DetermineFinalStatus();
            var finalCounts = BuildCountsJsonSnapshot();

            await using var finishTx = await connection.BeginTransactionAsync(cancellationToken);
            await _upsertRepository.UpdateBatchRunCompletionAsync(connection, batchId, finalCounts, finalStatus, finishTx, cancellationToken);
            await finishTx.CommitAsync(cancellationToken);
            _baseCountsJson = finalCounts;
            Logger.Info($"UPSERT完了: batch_id={batchId}, status={finalStatus}");
        }

        /// <summary>
        /// 途中失敗時にFAILEDでバッチを締める。
        /// </summary>
        public async Task MarkBatchFailedAsync(string batchId, CancellationToken cancellationToken = default)
        {
            var snapshot = BuildCountsJsonSnapshot();
            await using var connection = await _upsertRepository.OpenConnectionAsync(cancellationToken);
            await using var tx = await connection.BeginTransactionAsync(cancellationToken);
            await _upsertRepository.UpdateBatchRunCompletionAsync(connection, batchId, snapshot, "FAILED", tx, cancellationToken);
            await tx.CommitAsync(cancellationToken);
            _baseCountsJson = snapshot;
        }

        private string BuildCountsJsonSnapshot()
        {
            var root = string.IsNullOrWhiteSpace(_baseCountsJson)
                ? new JsonObject()
                : JsonNode.Parse(_baseCountsJson) as JsonObject ?? new JsonObject();

            root["UPSERT"] = new JsonObject
            {
                ["read"] = _counters.Read,
                ["insert"] = _counters.Insert,
                ["update"] = _counters.Update,
                ["skip"] = _counters.Skip,
                ["error"] = _counters.Error
            };

            return root.ToJsonString(JsonOptions);
        }

        private string DetermineFinalStatus()
        {
            var processed = _counters.Insert + _counters.Update + _counters.Skip;
            if (_counters.Error > 0 && processed == 0) return "FAILED";
            if (_counters.Error > 0) return "PARTIAL";
            return "COMPLETED";
        }
        private async Task<Dictionary<string, AttributeDefinition>> LoadDefinitionMapAsync(CancellationToken cancellationToken)
        {
            var definitions = await _attributeDefinitionRepository.GetAllAttrDefinitionAsync();
            return definitions.ToDictionary(def => def.AttrCd, def => def, StringComparer.OrdinalIgnoreCase);
        }

        private static long ParseGroupCompanyId(MCompany company)
        {
            if (long.TryParse(company.GroupCompanyId, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed))
            {
                return parsed;
            }

            throw new InvalidOperationException($"会社IDの変換に失敗しました: {company.GroupCompanyId}");
        }

        /// <summary>
        /// 商品識別コードを抽出する
        /// ・PRODUCT_CD（商品コード）と PRODUCT_MANAGEMENT_CD（製品コード）を属性データから取り出す
        /// ・商品コードは必須、製品コードは任意（KM会社の場合のみ使用）
        /// </summary>
        /// <param name="attrs">属性データのリスト</param>
        /// <param name="sourceProductCd">商品コード（出力）</param>
        /// <param name="sourceProductMgmtCd">製品コード（出力、任意）</param>
        /// <returns>商品コードが見つかれば true、見つからなければ false</returns>
        private static bool TryExtractIdentifiers(IEnumerable<ClProductAttr> attrs, out string? sourceProductCd, out string? sourceProductMgmtCd)
        {
            // 初期化：まだ何も見つかっていない状態にする
            sourceProductCd = null;
            sourceProductMgmtCd = null;

            // 全ての属性を1つずつ調べる
            foreach (var attr in attrs)
            {
                // PRODUCT_CD（商品コード）を探す
                if (string.Equals(attr.AttrCd, "PRODUCT_CD", StringComparison.OrdinalIgnoreCase))
                {
                    sourceProductCd = ExtractIdentifier(attr.ValueText, attr.SourceRaw, "source_product_cd");
                }
                // PRODUCT_MANAGEMENT_CD（製品コード）を探す
                else if (string.Equals(attr.AttrCd, "PRODUCT_MANAGEMENT_CD", StringComparison.OrdinalIgnoreCase))
                {
                    sourceProductMgmtCd = ExtractIdentifier(attr.ValueText, attr.SourceRaw, "source_product_management_cd");
                }
            }

            // 商品コードが見つかったかどうかを返す（製品コードは任意なので必須ではない）
            return !string.IsNullOrWhiteSpace(sourceProductCd);
        }

        private static string? ExtractIdentifier(string? valueText, string? fallback, string jsonKey)
        {
            if (!string.IsNullOrWhiteSpace(valueText))
            {
                try
                {
                    using var doc = JsonDocument.Parse(valueText);
                    if (doc.RootElement.TryGetProperty(jsonKey, out var element) && element.ValueKind == JsonValueKind.String)
                    {
                        return element.GetString();
                    }

                    if (doc.RootElement.ValueKind == JsonValueKind.String)
                    {
                        return doc.RootElement.GetString();
                    }
                }
                catch
                {
                    return valueText;
                }
            }

            return fallback;
        }

        /// <summary>
        /// 商品データの一括UPSERT処理（メイン処理）
        ///
        /// 【処理の流れ】
        /// 1. クレンジング済み属性データ（cl_product_attr）を全件取得
        /// 2. temp_row_id でグループ化（1商品 = 1グループ）
        /// 3. 各商品ごとに以下を実行：
        ///    a. 商品識別（m_product_ident）
        ///    b. 商品マスタ（m_product）のUPSERT
        ///    c. 商品EAV（m_product_eav）のUPSERT
        ///    d. 【KM会社のみ】製品マスタ（m_product_management）と製品EAV（m_product_management_eav）のUPSERT
        ///
        /// 【重要】
        /// ・1商品ごとに1トランザクションで処理
        /// ・エラーが発生しても他の商品の処理は継続
        /// </summary>
        private async Task ProcessProductsAsync(
            NpgsqlConnection connection,
            BatchRun batchRun,
            long defaultGroupCompanyId,
            IReadOnlyDictionary<string, AttributeDefinition> definitionMap,
            CancellationToken cancellationToken)
        {
            // ステップ1: クレンジング済み属性データを全件取得
            var attributes = await _upsertRepository.FetchProductAttributesAsync(connection, batchRun.BatchId, cancellationToken);
            // ステップ2: temp_row_id でグループ化（1商品 = 複数の属性）
            var grouped = attributes.GroupBy(attr => attr.TempRowId).ToList();
            Logger.Info($"商品属性取得: 属性件数={attributes.Count()}, 商品数(グループ)={grouped.Count}");

            // ステップ3: 商品ごとにループ処理
            foreach (var group in grouped)
            {
                _counters.IncrementRead(); // 読み込み件数をカウント
                var attrList = group.ToList();

                // ステップ3-1: 必須キー（PRODUCT_CD）のチェック
                if (!TryExtractIdentifiers(attrList, out var sourceProductCd, out var sourceProductMgmtCd))
                {
                    // 商品コードが見つからない場合はスキップ
                    _counters.IncrementSkip();
                    var codes = string.Join(",", attrList.Select(a => a.AttrCd));
                    Logger.Warn($"識別キー欠落によりスキップ: batch_id={batchRun.BatchId}, temp_row_id={group.Key}, attr_cds=[{codes}]");
                    await WriteRecordErrorAsync(
                        batchRun.BatchId,
                        group.Key.ToString(),
                        ErrorCodes.UPSERT_KEY_MISSING,
                        "必須識別キーが欠落しているため、このレコードをスキップしました。",
                        attrList);
                    continue; // 次の商品へ
                }

                var recordGroupCompanyId = defaultGroupCompanyId;
                Logger.Info($"識別解決(バッチ基準): temp_row_id={group.Key}, group_company_cd={batchRun.GroupCompanyCd}, group_company_id={recordGroupCompanyId}, source_product_cd={sourceProductCd}, source_product_management_cd={sourceProductMgmtCd}");

                // ステップ3-2: トランザクション開始（1商品ごとに1トランザクション）
                await using var tx = await connection.BeginTransactionAsync(cancellationToken);
                var currentErrorCd = ErrorCodes.UPSERT_UNKNOWN_ERROR;

                try
                {
                    // ステップ3-3: 商品識別（m_product_ident）
                    // 既存商品を探すか、新規商品IDを採番
                    currentErrorCd = ErrorCodes.IDENT_FAILED;
                    var ident = await EnsureProductIdentAsync(
                        connection,
                        tx,
                        batchRun.BatchId,
                        recordGroupCompanyId,
                        sourceProductCd!,
                        sourceProductMgmtCd,
                        cancellationToken);
                    Logger.Info($"製品ID決定: g_product_id={ident.GProductId}, is_new={ident.IsNew}");

                    // ステップ3-4: 商品マスタ（m_product）のUPSERT
                    // 固定列（g_brand_id, g_category_id, currency_cd 等）を書き込む
                    currentErrorCd = ErrorCodes.FIXED_COL_UPDATE_FAILED;
                    var existingProduct = await _upsertRepository.GetProductAsync(connection, ident.GProductId, tx, cancellationToken);

                    await UpsertProductAsync(
                        connection,
                        tx,
                        batchRun,
                        recordGroupCompanyId,
                        ident,
                        existingProduct,
                        attrList,
                        definitionMap,
                        cancellationToken);

                    // ステップ3-5: 商品EAV（m_product_eav）のUPSERT
                    // 可変属性（カテゴリ、ブランド、価格等）を書き込む
                    currentErrorCd = ErrorCodes.EAV_SYNC_FAILED;
                    await UpsertProductEavAsync(
                        connection,
                        tx,
                        batchRun,
                        ident.GProductId,
                        sourceProductCd!,
                        attrList,
                        definitionMap,
                        cancellationToken);

                    // ステップ3-6: 【KM会社のみ】製品マスタのUPSERT
                    // KM会社の場合のみ、製品マスタ（m_product_management）と製品EAV（m_product_management_eav）を作成
                    // 注意：製品マスタの処理でエラーが発生しても、商品マスタと商品EAVは正常にコミットする
                    if (string.Equals(batchRun.GroupCompanyCd, "KM", StringComparison.OrdinalIgnoreCase))
                    {
                        // SAVEPOINT を作成：製品マスタの処理が失敗しても、商品マスタの処理は保護される
                        await connection.ExecuteAsync("SAVEPOINT product_management_savepoint;", transaction: tx);

                        try
                        {
                            currentErrorCd = "PRODUCT_MANAGEMENT_FAILED";

                            // 重要：m_product の最新データを再取得（source_product_management_cd を含む）
                            // 理由：ステップ3-4で書き込んだ source_product_management_cd を取得するため
                            var latestProduct = await _upsertRepository.GetProductAsync(connection, ident.GProductId, tx, cancellationToken);

                            await UpsertProductManagementAsync(
                                connection,
                                tx,
                                batchRun,
                                recordGroupCompanyId,
                                latestProduct,
                                attrList,
                                definitionMap,
                                cancellationToken);

                            // 成功した場合は SAVEPOINT を解放
                            await connection.ExecuteAsync("RELEASE SAVEPOINT product_management_savepoint;", transaction: tx);
                        }
                        catch (Exception pmEx)
                        {
                            // エラーが発生した場合は SAVEPOINT までロールバック
                            // これにより、トランザクションは正常な状態に戻る
                            await connection.ExecuteAsync("ROLLBACK TO SAVEPOINT product_management_savepoint;", transaction: tx);

                            // 製品マスタの処理でエラーが発生しても、ログを記録して続行
                            // 商品マスタと商品EAVは正常にコミットする
                            Logger.Error($"製品マスタ処理エラー（商品マスタは正常）: batch_id={batchRun.BatchId}, temp_row_id={group.Key}, g_product_id={ident.GProductId}, error={pmEx.Message}");
                            Logger.Error($"スタックトレース: {pmEx.StackTrace}");
                            // エラーを記録するが、トランザクションは回滚しない
                        }
                    }

                    await tx.CommitAsync(cancellationToken);
                }
                catch (PostgresException pgEx)
                {
                    await tx.RollbackAsync(cancellationToken);
                    _counters.IncrementError();
                    Logger.Error($"UPSERT異常(PG): batch_id={batchRun.BatchId}, temp_row_id={group.Key}, error_cd={currentErrorCd}, state={pgEx.SqlState}, constraint={pgEx.ConstraintName}, message={pgEx.Message}");
                    await WriteRecordErrorAsync(
                        batchRun.BatchId,
                        group.Key.ToString(),
                        currentErrorCd,
                        $"UPSERT中にPGエラーが発生しました: state={pgEx.SqlState}, constraint={pgEx.ConstraintName}, message={pgEx.Message}",
                        attrList);
                }
                catch (Exception ex)
                {
                    await tx.RollbackAsync(cancellationToken);
                    _counters.IncrementError();
                    Logger.Error($"UPSERT異常: batch_id={batchRun.BatchId}, temp_row_id={group.Key}, error_cd={currentErrorCd}, message={ex.Message}");
                    await WriteRecordErrorAsync(
                        batchRun.BatchId,
                        group.Key.ToString(),
                        currentErrorCd,
                        $"UPSERT中に予期しないエラーが発生しました: {ex.Message}",
                        attrList);
                }
            }
        }        private async Task ProcessEventsAsync(NpgsqlConnection connection, BatchRun batchRun, CancellationToken cancellationToken)
        {
            var events = await _upsertRepository.FetchProductEventsAsync(connection, batchRun.BatchId, cancellationToken);
            Logger.Info($"イベント取得: 件数={events.Count()}");

            foreach (var evt in events)
            {
                _counters.IncrementRead();
                await using var tx = await connection.BeginTransactionAsync(cancellationToken);

                try
                {
                    await _upsertRepository.UpdateEventStatusAsync(connection, evt.TempRowEventId, "COMPLETED", tx, cancellationToken);
                    await tx.CommitAsync(cancellationToken);
                    _counters.IncrementUpdate();
                    Logger.Info($"イベントUPDATE: temp_row_event_id={evt.TempRowEventId}, status=COMPLETED");
                }
                catch (PostgresException pgEx)
                {
                    await tx.RollbackAsync(cancellationToken);
                    _counters.IncrementError();
                    Logger.Error($"イベント更新異常(PG): temp_row_event_id={evt.TempRowEventId}, state={pgEx.SqlState}, constraint={pgEx.ConstraintName}, message={pgEx.Message}");
                    await WriteRecordErrorAsync(batchRun.BatchId, evt.TempRowEventId.ToString(), ErrorCodes.EAV_SYNC_FAILED, pgEx.Message, null);
                }
                catch (Exception ex)
                {
                    await tx.RollbackAsync(cancellationToken);
                    _counters.IncrementError();
                    Logger.Error($"イベント更新異常: temp_row_event_id={evt.TempRowEventId}, message={ex.Message}");
                    await WriteRecordErrorAsync(batchRun.BatchId, evt.TempRowEventId.ToString(), ErrorCodes.EAV_SYNC_FAILED, ex.Message, null);
                }
            }
        }private Task WriteRecordErrorAsync(
            string batchId,
            string? recordRef,
            string errorCd,
            string errorDetail,
            IEnumerable<ClProductAttr>? attrs)
        {
            try
            {
                var fragment = attrs == null
                    ? recordRef
                    : JsonSerializer.Serialize(
                        attrs.Select(a => new
                        {
                            a.AttrCd,
                            a.AttrSeq,
                            a.ValueText,
                            a.ValueCd,
                            a.QualityStatus
                        }),
                        JsonOptions);

                var error = new RecordError
                {
                    ErrorId = Guid.NewGuid(),
                    BatchId = batchId,
                    Step = "UPSERT",
                    RecordRef = recordRef ?? string.Empty,
                    ErrorCd = errorCd,
                    ErrorDetail = errorDetail,
                    RawFragment = fragment ?? string.Empty,
                    CreAt = DateTime.UtcNow,
                    UpdAt = DateTime.UtcNow
                };

                return _recordErrorRepository.InsertAsync(error);
            }
            catch
            {
                return Task.CompletedTask;
            }
        }

        private async Task<EnsureIdentResult> EnsureProductIdentAsync(
            NpgsqlConnection connection,
            NpgsqlTransaction transaction,
            string batchId,
            long groupCompanyId,
            string sourceProductCd,
            string? sourceProductMgmtCd,
            CancellationToken cancellationToken)
        {
            var existing = await _upsertRepository.FindActiveProductIdAsync(
                connection,
                groupCompanyId,
                sourceProductCd,
                transaction,
                cancellationToken);

            if (existing.HasValue)
            {
                return new EnsureIdentResult(existing.Value, false);
            }

            var newProductId = await _upsertRepository.GetNextProductIdAsync(connection, transaction, cancellationToken);
            var newIdentId = await _upsertRepository.GetNextProductIdentIdAsync(connection, transaction, cancellationToken);

            var ident = new MProductIdent
            {
                IdentId = newIdentId,
                GProductId = newProductId,
                GroupCompanyId = groupCompanyId,
                SourceProductCd = sourceProductCd,
                SourceProductManagementCd = sourceProductMgmtCd,
                BatchId = batchId
            };

            var inserted = await _upsertRepository.InsertProductIdentAsync(connection, ident, transaction, cancellationToken);
            if (inserted)
            {
                return new EnsureIdentResult(newProductId, true);
            }

            var retry = await _upsertRepository.FindActiveProductIdAsync(
                connection,
                groupCompanyId,
                sourceProductCd,
                transaction,
                cancellationToken);

            if (retry.HasValue)
            {
                Logger.Info($"識別子競合→既存IDを使用: g_product_id={retry.Value}");
                return new EnsureIdentResult(retry.Value, false);
            }

            throw new InvalidOperationException($"識別子の確保に失敗しました: group_company_id={groupCompanyId}, source_product_cd={sourceProductCd}");
        }
        private async Task UpsertProductAsync(
            NpgsqlConnection connection,
            NpgsqlTransaction transaction,
            BatchRun batchRun,
            long groupCompanyId,
            EnsureIdentResult ident,
            MProduct? existingProduct,
            IReadOnlyCollection<ClProductAttr> attrs,
            IReadOnlyDictionary<string, AttributeDefinition> definitionMap,
            CancellationToken cancellationToken)
        {
            var fixedColumns = BuildProductColumnValues(attrs, definitionMap);
            var ensuredBrandId = await EnsureBrandIdAsync(attrs, fixedColumns, cancellationToken);
            var ensuredCategoryId = await EnsureCategoryIdAsync(attrs, fixedColumns, cancellationToken);

            if (ident.IsNew && existingProduct == null)
            {
                if (!ensuredCategoryId.HasValue)
                {
                    var tempRowId = attrs.FirstOrDefault()?.TempRowId.ToString() ?? "UNKNOWN";
                    throw new InvalidOperationException($"CATEGORY_1 から g_category_id を決定できません (temp_row_id={tempRowId})");
                }

                var newProduct = new MProduct
                {
                    GProductId = ident.GProductId,
                    GProductCd = GenerateGProductCd(ident.GProductId),
                    UnitNo = 1,
                    GroupCompanyId = fixedColumns.TryGetValue("group_company_id", out var company) ? NormalizeLong(company) ?? groupCompanyId : groupCompanyId,
                    SourceProductCd = fixedColumns.TryGetValue("source_product_cd", out var spc) ? spc as string : null,
                    SourceProductManagementCd = fixedColumns.TryGetValue("source_product_management_cd", out var spm) ? spm as string : null,
                    GBrandId = ensuredBrandId,
                    GCategoryId = ensuredCategoryId,
                    CurrencyCd = fixedColumns.TryGetValue("currency_cd", out var currency) ? currency as string : null,
                    DisplayPriceInclTax = fixedColumns.TryGetValue("display_price_incl_tax", out var price) ? NormalizeDecimal(price) : null,
                    ProductStatusCd = "PRODUCT_STATUS_UNKNOWN",
                    NewUsedKbnCd = "PRODUCT_CONDITION_UNKNOWN",
                    StockExistenceCd = "STOCK_UNKNOWN",
                    SaleStatusCd = "SALE_UNKNOWN",
                    IsActive = true
                };

                await _upsertRepository.InsertProductAsync(connection, newProduct, transaction, cancellationToken);
                _counters.IncrementInsert();
                Logger.Info($"商品INSERT: g_product_id={newProduct.GProductId}, g_brand_id={newProduct.GBrandId?.ToString() ?? "null"}, g_category_id={newProduct.GCategoryId?.ToString() ?? "null"}, currency_cd={newProduct.CurrencyCd ?? "null"}, display_price_incl_tax={newProduct.DisplayPriceInclTax?.ToString() ?? "null"}");
            }
            else if (existingProduct != null)
            {
                var diff = BuildProductUpdateDiff(existingProduct, fixedColumns);
                if (diff.Count > 0)
                {
                    await _upsertRepository.UpdateProductAsync(connection, existingProduct.GProductId, diff, transaction, cancellationToken);
                    _counters.IncrementUpdate();
                    Logger.Info($"商品UPDATE: g_product_id={existingProduct.GProductId}, 更新カラム=[{string.Join(",", diff.Keys)}]");
                }
                else
                {
                    _counters.IncrementSkip();
                    Logger.Info($"商品SKIP: g_product_id={existingProduct.GProductId}, 差分なし");
                }
            }
        }
        private static Dictionary<string, object?> BuildProductColumnValues(
            IEnumerable<ClProductAttr> attrs,
            IReadOnlyDictionary<string, AttributeDefinition> definitionMap)
        {
            var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

            foreach (var attr in attrs)
            {
                if (!definitionMap.TryGetValue(attr.AttrCd, out var definition))
                {
                    continue;
                }

                if (!definition.IsGoldenProduct || string.IsNullOrWhiteSpace(definition.TargetColumn))
                {
                    continue;
                }

                var rawValue = ResolveAttributeValue(attr, definition);
                if (rawValue == null)
                {
                    continue;
                }

                result[definition.TargetColumn] = NormalizeProductColumnValue(definition.TargetColumn, rawValue);
            }

            return result;
        }

        private async Task<long?> EnsureBrandIdAsync(
            IReadOnlyCollection<ClProductAttr> attrs,
            Dictionary<string, object?> fixedColumns,
            CancellationToken cancellationToken)
        {
            if (fixedColumns.TryGetValue("g_brand_id", out var existingValue))
            {
                var normalized = NormalizeLong(existingValue);
                if (normalized.HasValue && normalized.Value > 0)
                {
                    fixedColumns["g_brand_id"] = normalized.Value;
                    return normalized;
                }
            }

            var resolved = await ResolveBrandIdFromAttributesAsync(attrs, cancellationToken);
            if (resolved.HasValue)
            {
                fixedColumns["g_brand_id"] = resolved.Value;
            }

            return resolved;
        }

        private async Task<long?> ResolveBrandIdFromAttributesAsync(
            IEnumerable<ClProductAttr> attrs,
            CancellationToken cancellationToken)
        {
            var brandAttr = attrs.FirstOrDefault(a => a.AttrCd.Equals("BRAND", StringComparison.OrdinalIgnoreCase));
            if (brandAttr == null)
            {
                return null;
            }

            var brandCode = ExtractBrandCode(brandAttr);
            if (string.IsNullOrWhiteSpace(brandCode))
            {
                return null;
            }

            return await _brandRepository.GetIdByCodeAsync(brandCode, cancellationToken);
        }

        private static string? ExtractBrandCode(ClProductAttr attr)
        {
            if (!string.IsNullOrWhiteSpace(attr.ValueCd))
            {
                return attr.ValueCd;
            }

            if (!string.IsNullOrWhiteSpace(attr.ValueText))
            {
                var text = attr.ValueText.Trim();
                if (text.StartsWith("{", StringComparison.Ordinal))
                {
                    try
                    {
                        using var doc = JsonDocument.Parse(text);
                        if (doc.RootElement.ValueKind == JsonValueKind.String)
                        {
                            return doc.RootElement.GetString();
                        }

                        if (doc.RootElement.TryGetProperty("g_brand_cd", out var gBrandNode) && gBrandNode.ValueKind == JsonValueKind.String)
                        {
                            return gBrandNode.GetString();
                        }

                        if (doc.RootElement.TryGetProperty("brand_cd", out var brandNode) && brandNode.ValueKind == JsonValueKind.String)
                        {
                            return brandNode.GetString();
                        }
                    }
                    catch
                    {
                        return text;
                    }
                }

                return text;
            }

            if (!string.IsNullOrWhiteSpace(attr.SourceRaw))
            {
                return attr.SourceRaw;
            }

            return null;
        }

        private async Task<long?> EnsureCategoryIdAsync(
            IReadOnlyCollection<ClProductAttr> attrs,
            Dictionary<string, object?> fixedColumns,
            CancellationToken cancellationToken)
        {
            if (fixedColumns.TryGetValue("g_category_id", out var existingValue))
            {
                var normalized = NormalizeLong(existingValue);
                if (normalized.HasValue && normalized.Value > 0)
                {
                    fixedColumns["g_category_id"] = normalized.Value;
                    return normalized;
                }
            }

            var resolved = await ResolveCategoryIdFromAttributesAsync(attrs, cancellationToken);
            if (resolved.HasValue)
            {
                fixedColumns["g_category_id"] = resolved.Value;
                return resolved;
            }

            return null;
        }

        private async Task<long?> ResolveCategoryIdFromAttributesAsync(
            IEnumerable<ClProductAttr> attrs,
            CancellationToken cancellationToken)
        {
            var categoryAttr = attrs.FirstOrDefault(a => a.AttrCd.Equals("CATEGORY_1", StringComparison.OrdinalIgnoreCase));
            if (categoryAttr == null)
            {
                return null;
            }

            var categoryCode = ExtractCategoryCode(categoryAttr);
            if (string.IsNullOrWhiteSpace(categoryCode))
            {
                return null;
            }

            return await _categoryRepository.GetIdByCodeAsync(categoryCode, cancellationToken);
        }

        private static string? ExtractCategoryCode(ClProductAttr attr)
        {
            if (!string.IsNullOrWhiteSpace(attr.ValueCd))
            {
                return attr.ValueCd;
            }

            if (!string.IsNullOrWhiteSpace(attr.ValueText))
            {
                var text = attr.ValueText.Trim();
                if (text.StartsWith("{", StringComparison.Ordinal))
                {
                    try
                    {
                        using var doc = JsonDocument.Parse(text);
                        if (doc.RootElement.ValueKind == JsonValueKind.String)
                        {
                            return doc.RootElement.GetString();
                        }

                        if (doc.RootElement.TryGetProperty("g_category_cd", out var gCatNode) && gCatNode.ValueKind == JsonValueKind.String)
                        {
                            return gCatNode.GetString();
                        }

                        if (doc.RootElement.TryGetProperty("category_cd", out var catNode) && catNode.ValueKind == JsonValueKind.String)
                        {
                            return catNode.GetString();
                        }
                    }
                    catch
                    {
                        return text;
                    }
                }

                return text;
            }

            if (!string.IsNullOrWhiteSpace(attr.SourceRaw))
            {
                return attr.SourceRaw;
            }

            return null;
        }

        private static object? ResolveAttributeValue(ClProductAttr attr, AttributeDefinition definition)
        {
            if (!string.IsNullOrWhiteSpace(definition.TargetColumn))
            {
                var column = definition.TargetColumn.ToLowerInvariant();
                if (column is "source_product_cd" or "source_product_management_cd")
                {
                    return ExtractIdentifier(attr.ValueText, attr.SourceRaw, column);
                }
            }

            var dataType = definition.DataType?.ToUpperInvariant();
            return dataType switch
            {
                "NUM" or "NUMBER" => attr.ValueNum,
                "DATE" => attr.ValueDate,
                "LIST" or "REF" => attr.ValueCd ?? attr.ValueText,
                _ => ResolveFallbackValue(attr)
            };
        }

        private static object? ResolveFallbackValue(ClProductAttr attr)
        {
            if (!string.IsNullOrWhiteSpace(attr.ValueText)) return attr.ValueText;
            if (!string.IsNullOrWhiteSpace(attr.SourceRaw)) return attr.SourceRaw;
            if (!string.IsNullOrWhiteSpace(attr.ValueCd)) return attr.ValueCd;
            if (attr.ValueNum.HasValue) return attr.ValueNum.Value;
            if (attr.ValueDate.HasValue) return attr.ValueDate.Value;
            return null;
        }

        private static object? NormalizeProductColumnValue(string column, object? value)
        {
            if (value == null) return null;

            if (column.EndsWith("_id", StringComparison.OrdinalIgnoreCase))
            {
                return NormalizeLong(value);
            }

            if (column.Contains("price", StringComparison.OrdinalIgnoreCase) ||
                column.EndsWith("_amount", StringComparison.OrdinalIgnoreCase))
            {
                return NormalizeDecimal(value);
            }

            return value;
        }

        private static long? NormalizeLong(object? value)
        {
            if (value == null) return null;
            if (value is long l) return l;
            if (value is int i) return i;
            if (value is decimal dec) return (long)dec;
            if (value is string s && long.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed))
            {
                return parsed;
            }

            return null;
        }

        private static decimal? NormalizeDecimal(object? value)
        {
            if (value == null) return null;
            if (value is decimal dec) return dec;
            if (value is double dbl) return (decimal)dbl;
            if (value is string s && decimal.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed))
            {
                return parsed;
            }

            return null;
        }

        private static Dictionary<string, object?> BuildProductUpdateDiff(MProduct existing, Dictionary<string, object?> current)
        {
            var diff = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

            foreach (var (column, value) in current)
            {
                var existingValue = GetExistingProductValue(existing, column);
                if (!AreValuesEqual(existingValue, value))
                {
                    diff[column] = value;
                }
            }

            return diff;
        }

        private static object? GetExistingProductValue(MProduct product, string column)
        {
            return column.ToLowerInvariant() switch
            {
                "source_product_cd" => product.SourceProductCd,
                "source_product_management_cd" => product.SourceProductManagementCd,
                "g_brand_id" => product.GBrandId,
                "g_category_id" => product.GCategoryId,
                "currency_cd" => product.CurrencyCd,
                "display_price_incl_tax" => product.DisplayPriceInclTax,
                _ => null
            };
        }

        private static bool AreValuesEqual(object? left, object? right)
        {
            if (left == null && right == null) return true;
            if (left == null || right == null) return false;

            if (left is decimal dl && right is decimal dr) return dl == dr;
            if (left is long ll && right is long lr) return ll == lr;
            if (left is int il && right is int ir) return il == ir;
            if (left is DateTime dtl && right is DateTime dtr) return dtl == dtr;

            return string.Equals(left.ToString(), right.ToString(), StringComparison.OrdinalIgnoreCase);
        }

        private static string GenerateGProductCd(long gProductId)
        {
            const long baseNo = 1_000_000_000_000;
            return (baseNo + gProductId).ToString(CultureInfo.InvariantCulture);
        }
        private async Task UpsertProductEavAsync(
            NpgsqlConnection connection,
            NpgsqlTransaction transaction,
            BatchRun batchRun,
            long gProductId,
            string sourceProductCd,
            IReadOnlyCollection<ClProductAttr> attrs,
            IReadOnlyDictionary<string, AttributeDefinition> definitionMap,
            CancellationToken cancellationToken)
        {
            var existingMap = await _upsertRepository.GetProductEavMapAsync(connection, gProductId, transaction, cancellationToken);
            var untouchedKeys = new HashSet<(string attrCd, short attrSeq)>(existingMap.Keys);

            foreach (var attr in attrs)
            {
                if (!definitionMap.TryGetValue(attr.AttrCd, out var definition))
                {
                    _counters.IncrementSkip();
                    Logger.Warn($"EAVスキップ(定義なし): g_product_id={gProductId}, attr_cd={attr.AttrCd}");
                    continue;
                }

                if (!definition.IsGoldenAttrEav)
                {
                    Logger.Info($"EAV対象外: g_product_id={gProductId}, attr_cd={attr.AttrCd}, is_golden_attr_eav={definition.IsGoldenAttrEav}");
                    continue;
                }

                var attrSeq = attr.AttrSeq > 0 ? attr.AttrSeq : (short)1;
                var key = (attr.AttrCd, attrSeq);
                untouchedKeys.Remove(key);

                var existing = existingMap.TryGetValue(key, out var eav) ? eav : null;
                var payload = BuildEavPayload(batchRun, sourceProductCd, attr, definition, existing);

                if (existing == null)
                {
                    await _upsertRepository.InsertProductEavAsync(connection, payload.ToEntity(gProductId, attrSeq), transaction, cancellationToken);
                    _counters.IncrementInsert();
                    Logger.Info($"EAV INSERT: g_product_id={gProductId}, attr_cd={attr.AttrCd}, attr_seq={attrSeq}, data_type={definition.DataType}");
                    continue;
                }

                var diff = payload.BuildDiff(existing);
                if (diff.Count > 0)
                {
                    await _upsertRepository.UpdateProductEavAsync(connection, gProductId, attr.AttrCd, attrSeq, diff, transaction, cancellationToken);
                    _counters.IncrementUpdate();
                    Logger.Info($"EAV UPDATE: g_product_id={gProductId}, attr_cd={attr.AttrCd}, attr_seq={attrSeq}, 更新カラム=[{string.Join(",", diff.Keys)}]");
                }
                else if (!existing.IsActive)
                {
                    diff["is_active"] = true;
                    await _upsertRepository.UpdateProductEavAsync(connection, gProductId, attr.AttrCd, attrSeq, diff, transaction, cancellationToken);
                    _counters.IncrementUpdate();
                    Logger.Info($"EAV REACTIVATE: g_product_id={gProductId}, attr_cd={attr.AttrCd}, attr_seq={attrSeq}");
                }
                else
                {
                    _counters.IncrementSkip();
                    Logger.Info($"EAV SKIP: g_product_id={gProductId}, attr_cd={attr.AttrCd}, attr_seq={attrSeq}, 変更なし");
                }
            }

            foreach (var key in untouchedKeys)
            {
                await _upsertRepository.MarkProductEavInactiveAsync(connection, gProductId, key.attrCd, key.attrSeq, transaction, cancellationToken);
                _counters.IncrementUpdate();
                Logger.Info($"EAV DEACTIVATE: g_product_id={gProductId}, attr_cd={key.attrCd}, attr_seq={key.attrSeq}");
            }
        }

        private static EavPayload BuildEavPayload(
            BatchRun batchRun,
            string sourceProductCd,
            ClProductAttr attr,
            AttributeDefinition definition,
            MProductEav? existing)
        {
            var attrSeq = attr.AttrSeq > 0 ? attr.AttrSeq : (short)1;
            var provenance = new
            {
                source_system = batchRun.GroupCompanyCd,
                ingest_profile = $"{batchRun.GroupCompanyCd}_{batchRun.DataKind}",
                idem_key = $"{batchRun.BatchId}:{batchRun.GroupCompanyCd}:{sourceProductCd}:{attr.AttrCd}:{attrSeq}",
                rule_version = attr.RuleVersion,
                dict_hi = ExtractDictHi(attr.QualityDetailJson)
            };

            var provenanceJson = ProvenanceHelper.AppendProvenanceJson(
                existing?.ProvenanceJson,
                provenance);

            return new EavPayload(
                definition.DataType,
                attr,
                definition.ProductUnitCd,
                provenanceJson,
                existing);
        }

        private static string? ExtractDictHi(string? qualityDetailJson)
        {
            if (string.IsNullOrWhiteSpace(qualityDetailJson)) return null;

            try
            {
                using var doc = JsonDocument.Parse(qualityDetailJson);
                if (doc.RootElement.TryGetProperty("dict_hi", out var element) && element.ValueKind == JsonValueKind.String)
                {
                    return element.GetString();
                }
            }
            catch
            {
                return null;
            }

            return null;
        }
        private async Task<long> ResolveRecordGroupCompanyIdAsync(
            string fallbackGroupCompanyCd,
            IEnumerable<ClProductAttr> attrs,
            long fallbackGroupCompanyId,
            CancellationToken cancellationToken)
        {
            var gpAttr = attrs.FirstOrDefault(a => string.Equals(a.AttrCd, "GP_CD", StringComparison.OrdinalIgnoreCase));
            var candidateCd = ExtractGroupCompanyCd(gpAttr);

            if (string.IsNullOrWhiteSpace(candidateCd))
            {
                return fallbackGroupCompanyId;
            }

            if (_companyIdCache.TryGetValue(candidateCd, out var cached))
            {
                return cached;
            }

            var company = await _companyRepository.GetActiveCompanyAsync(candidateCd)
                ?? throw new InvalidOperationException($"GP会社コード {candidateCd} に対応する会社が見つかりません。");

            var parsedId = ParseGroupCompanyId(company);
            _companyIdCache[candidateCd] = parsedId;
            return parsedId;
        }

        private static string? ExtractGroupCompanyCd(ClProductAttr? attr)
        {
            if (attr == null) return null;

            if (!string.IsNullOrWhiteSpace(attr.ValueCd))
            {
                return attr.ValueCd;
            }

            if (!string.IsNullOrWhiteSpace(attr.ValueText))
            {
                try
                {
                    using var doc = JsonDocument.Parse(attr.ValueText);
                    if (doc.RootElement.ValueKind == JsonValueKind.String)
                    {
                        return doc.RootElement.GetString();
                    }

                    if (doc.RootElement.TryGetProperty("group_company_cd", out var node) && node.ValueKind == JsonValueKind.String)
                    {
                        return node.GetString();
                    }

                    if (doc.RootElement.TryGetProperty("gp_cd", out var gpNode) && gpNode.ValueKind == JsonValueKind.String)
                    {
                        return gpNode.GetString();
                    }
                }
                catch
                {
                    return attr.ValueText;
                }
            }

            if (!string.IsNullOrWhiteSpace(attr.SourceRaw))
            {
                return attr.SourceRaw;
            }

            return null;
        }

        private static bool AreJsonEqual(string? left, string? right)
        {
            if (string.IsNullOrWhiteSpace(left) && string.IsNullOrWhiteSpace(right)) return true;
            if (string.IsNullOrWhiteSpace(left) || string.IsNullOrWhiteSpace(right)) return false;

            try
            {
                var leftNode = JsonNode.Parse(left);
                var rightNode = JsonNode.Parse(right);
                return JsonNode.DeepEquals(leftNode, rightNode);
            }
            catch
            {
                return string.Equals(left, right, StringComparison.Ordinal);
            }
        }

        private sealed class EavPayload
        {
            private readonly string? _dataType;
            private readonly ClProductAttr _attr;
            private readonly string? _unitCd;
            private readonly string _provenanceJson;
            private readonly MProductEav? _existing;

            public EavPayload(
                string? dataType,
                ClProductAttr attr,
                string? unitCd,
                string provenanceJson,
                MProductEav? existing)
            {
                _dataType = dataType;
                _attr = attr;
                _unitCd = unitCd;
                _provenanceJson = provenanceJson;
                _existing = existing;
            }

            public MProductEav ToEntity(long gProductId, short attrSeq)
            {
                return new MProductEav
                {
                    GProductId = gProductId,
                    AttrCd = _attr.AttrCd,
                    AttrSeq = attrSeq,
                    ValueText = ResolveText(),
                    ValueNum = ResolveNum(),
                    ValueDate = ResolveDate(),
                    ValueCd = ResolveCd(),
                    UnitCd = _unitCd,
                    QualityStatus = _attr.QualityStatus,
                    QualityDetailJson = _attr.QualityDetailJson,
                    ProvenanceJson = _provenanceJson,
                    BatchId = _attr.BatchId,
                    IsActive = true
                };
            }

            public Dictionary<string, object?> BuildDiff(MProductEav existing)
            {
                var diff = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

                var newText = ResolveText();
                if (!AreValuesEqual(existing.ValueText, newText))
                {
                    diff["value_text"] = newText;
                }

                var newNum = ResolveNum();
                if (!AreValuesEqual(existing.ValueNum, newNum))
                {
                    diff["value_num"] = newNum;
                }

                var newDate = ResolveDate();
                if (!AreValuesEqual(existing.ValueDate, newDate))
                {
                    diff["value_date"] = newDate;
                }

                var newCd = ResolveCd();
                if (!AreValuesEqual(existing.ValueCd, newCd))
                {
                    diff["value_cd"] = newCd;
                }

                if (!AreValuesEqual(existing.UnitCd, _unitCd))
                {
                    diff["unit_cd"] = _unitCd;
                }

                if (!AreValuesEqual(existing.QualityStatus, _attr.QualityStatus))
                {
                    diff["quality_status"] = _attr.QualityStatus;
                }

                if (!AreJsonEqual(existing.QualityDetailJson, _attr.QualityDetailJson))
                {
                    diff["quality_detail_json"] = _attr.QualityDetailJson;
                }

                if (!AreJsonEqual(existing.ProvenanceJson, _provenanceJson))
                {
                    diff["provenance_json"] = _provenanceJson;
                }

                diff["batch_id"] = _attr.BatchId;
                diff["is_active"] = true;

                return diff;
            }

            private string? ResolveText()
            {
                if (string.Equals(_dataType, "TEXT", StringComparison.OrdinalIgnoreCase))
                {
                    return _attr.ValueText ?? _attr.SourceRaw;
                }

                if (string.Equals(_dataType, "LIST", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(_dataType, "REF", StringComparison.OrdinalIgnoreCase))
                {
                    return _attr.ValueText;
                }

                return null;
            }

            private decimal? ResolveNum()
            {
                if (string.Equals(_dataType, "NUM", StringComparison.OrdinalIgnoreCase))
                {
                    return _attr.ValueNum;
                }

                return null;
            }

            private DateTime? ResolveDate()
            {
                if (string.Equals(_dataType, "DATE", StringComparison.OrdinalIgnoreCase))
                {
                    return _attr.ValueDate;
                }

                return null;
            }

            private string? ResolveCd()
            {
                if (string.Equals(_dataType, "LIST", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(_dataType, "REF", StringComparison.OrdinalIgnoreCase))
                {
                    return _attr.ValueCd;
                }

                return null;
            }
        }

        /// <summary>
        /// 製品マスタ（m_product_management）と製品EAV（m_product_management_eav）をUPSERTする
        ///
        /// 【実行条件】
        /// ・KM会社のデータのみ
        /// ・source_product_management_cd（製品コード）が存在する場合のみ
        ///
        /// 【処理内容】
        /// 1. 製品識別：(group_company_id, source_product_management_cd, is_provisional=FALSE) で既存製品を検索
        /// 2-A. 見つからない場合：新規製品を作成
        /// 2-B. 見つかった場合：既存製品を更新
        /// 3. 製品EAV（m_product_management_eav）をUPSERT
        ///
        /// 【データの取得元】
        /// ・g_brand_id：m_product.g_brand_id から取得
        /// ・g_category_id：m_product.g_category_id から取得
        /// ・description_text：cl_product_attr の CATALOG_DESC 属性から取得
        /// ・source_product_cd：m_product.g_product_id（商品の内部ID）
        /// ・is_provisional：KM会社の場合は常に FALSE（正式製品）
        /// </summary>
        private async Task UpsertProductManagementAsync(
            NpgsqlConnection connection,
            NpgsqlTransaction tx,
            BatchRun batchRun,
            long groupCompanyId,
            MProduct? existingProduct,
            IEnumerable<ClProductAttr> attrs,
            IReadOnlyDictionary<string, AttributeDefinition> definitionMap,
            CancellationToken cancellationToken)
        {
            // チェック1: m_product データが存在するか
            if (existingProduct == null)
            {
                Logger.Warn($"製品マスタスキップ: m_product が NULL です（データ整合性エラーの可能性）");
                return;
            }

            // チェック2: source_product_management_cd（製品コード）が存在するか
            // 注意：KM会社の商品でも、製品コードがない商品は製品マスタに登録しない
            if (string.IsNullOrWhiteSpace(existingProduct.SourceProductManagementCd))
            {
                Logger.Info($"製品マスタスキップ: source_product_management_cd が NULL または空です (g_product_id={existingProduct.GProductId}, source_product_cd={existingProduct.SourceProductCd})");
                Logger.Info("【ヒント】データに PRODUCT_MANAGEMENT_CD 属性が含まれているか確認してください");
                return;
            }

            var sourceProductManagementCd = existingProduct.SourceProductManagementCd;
            Logger.Info($"製品マスタUPSERT開始: source_product_management_cd={sourceProductManagementCd}, g_product_id={existingProduct.GProductId}");

            // ステップ1: 既存の製品レコードを検索
            // キー：(group_company_id, source_product_management_cd, is_provisional=FALSE)
            var existingId = await _productManagementRepository.FindActiveProductManagementIdAsync(
                connection,
                groupCompanyId,
                sourceProductManagementCd,
                tx,
                cancellationToken);

            long gProductManagementId;
            bool isNew;

            if (existingId == null)
            {
                // ========================================
                // パターンA：新規製品の作成
                // ========================================
                // ステップ2-A-1: 新しい製品ID（g_product_management_id）を採番
                gProductManagementId = await _productManagementRepository.GetNextProductManagementIdAsync(
                    connection,
                    tx,
                    cancellationToken);

                // ステップ2-A-2: データの必須チェック
                // g_category_id は NOT NULL 制約があるため、NULL の場合はスキップ
                if (!existingProduct.GCategoryId.HasValue)
                {
                    Logger.Error($"製品マスタ作成失敗: g_category_id が NULL です (g_product_id={existingProduct.GProductId})");
                    Logger.Error("【原因】CATEGORY_1 属性が見つからないか、カテゴリIDへの変換に失敗しました");
                    return; // エラーログを記録して終了（例外は投げない）
                }

                // ステップ2-A-3: データの準備
                // description_text：CATALOG_DESC 属性から製品説明を取得
                var descriptionText = GetCatalogDesc(attrs);
                Logger.Info($"製品マスタ作成データ: g_brand_id={existingProduct.GBrandId?.ToString() ?? "NULL"}, g_category_id={existingProduct.GCategoryId}, description_text={descriptionText ?? "NULL"}, source_product_cd={existingProduct.GProductId}");

                // ステップ2-A-4: 製品マスタオブジェクトの作成
                var newProductManagement = new MProductManagement
                {
                    GProductManagementId = gProductManagementId,           // 採番した製品ID
                    GroupCompanyId = groupCompanyId,                       // 会社ID
                    SourceProductManagementCd = sourceProductManagementCd, // 製品コード（キー）
                    GBrandId = existingProduct.GBrandId,                   // ブランドID（m_product から）
                    GCategoryId = existingProduct.GCategoryId.Value,       // カテゴリID（m_product から）
                    DescriptionText = descriptionText,                     // 製品説明（CATALOG_DESC から）
                    IsProvisional = false,                                 // KM会社は正式製品（FALSE固定）
                    SourceProductCd = existingProduct.GProductId,          // 元の商品ID
                    ProvenanceJson = BuildProductManagementProvenance(batchRun, sourceProductManagementCd), // 由来情報
                    BatchId = batchRun.BatchId,                            // バッチID
                    IsActive = true                                        // 有効フラグ
                };

                // ステップ2-A-5: データベースに挿入
                await _productManagementRepository.InsertProductManagementAsync(
                    connection,
                    newProductManagement,
                    tx,
                    cancellationToken);

                isNew = true;
                Logger.Info($"製品マスタ新規作成: g_product_management_id={gProductManagementId}, source_product_management_cd={sourceProductManagementCd}");
            }
            else
            {
                // ========================================
                // パターンB：既存製品の更新
                // ========================================
                // ステップ2-B-1: 既存製品のIDを使用
                gProductManagementId = existingId.Value;

                // ステップ2-B-2: データの必須チェック
                if (!existingProduct.GCategoryId.HasValue)
                {
                    Logger.Error($"製品マスタ更新失敗: g_category_id が NULL です (g_product_management_id={gProductManagementId})");
                    Logger.Error("【原因】CATEGORY_1 属性が見つからないか、カテゴリIDへの変換に失敗しました");
                    return; // エラーログを記録して終了（例外は投げない）
                }

                // ステップ2-B-3: 更新データの準備
                var descriptionText = GetCatalogDesc(attrs);

                // ステップ2-B-4: 更新する列の指定
                // 注意：キー列（g_product_management_id, group_company_id, source_product_management_cd）は更新しない
                var updateValues = new Dictionary<string, object?>
                {
                    ["g_brand_id"] = existingProduct.GBrandId,                         // ブランドID（最新値に更新）
                    ["g_category_id"] = existingProduct.GCategoryId.Value,             // カテゴリID（最新値に更新）
                    ["description_text"] = descriptionText,                            // 製品説明（最新値に更新）
                    ["provenance_json"] = BuildProductManagementProvenance(batchRun, sourceProductManagementCd), // 由来情報（マージ）
                    ["batch_id"] = batchRun.BatchId                                    // 最終更新バッチID
                };

                // ステップ2-B-5: データベースを更新
                await _productManagementRepository.UpdateProductManagementAsync(
                    connection,
                    gProductManagementId,
                    updateValues,
                    tx,
                    cancellationToken);

                isNew = false;
                Logger.Info($"製品マスタ更新: g_product_management_id={gProductManagementId}, source_product_management_cd={sourceProductManagementCd}, g_brand_id={existingProduct.GBrandId?.ToString() ?? "NULL"}, g_category_id={existingProduct.GCategoryId}, description_text={descriptionText ?? "NULL"}");
            }

            // 製品EAVのUPSERT
            await UpsertProductManagementEavAsync(
                connection,
                tx,
                batchRun,
                gProductManagementId,
                attrs,
                definitionMap,
                cancellationToken);

            if (isNew)
            {
                _counters.IncrementInsert();
            }
            else
            {
                _counters.IncrementUpdate();
            }
        }

        /// <summary>
        /// 製品EAVマスタ（m_product_management_eav）をUPSERTする
        ///
        /// 【処理内容】
        /// 1. 既存のEAVデータを全件取得
        /// 2. 入力属性を1つずつ処理：
        ///    ・is_golden_eav = TRUE の属性のみが対象
        ///    ・既存データがあれば差分更新（値が変わっている列のみ更新）
        ///    ・既存データがなければ新規作成
        /// 3. 今回のバッチに含まれなかった属性を is_active = FALSE にする
        ///
        /// 【差分更新の仕組み】
        /// ・value_text, value_num, value_date, value_cd を個別にチェック
        /// ・data_type に応じて該当する列のみを更新
        /// ・例：data_type='TEXT' なら value_text のみ更新、他の value_* 列は触らない
        ///
        /// 【is_active フラグの管理】
        /// ・新規作成時：is_active = TRUE
        /// ・更新時：is_active = TRUE に戻す（過去に FALSE になっていても）
        /// ・未出現時：is_active = FALSE に設定（再出現したら TRUE に戻る）
        /// </summary>
        private async Task UpsertProductManagementEavAsync(
            NpgsqlConnection connection,
            NpgsqlTransaction tx,
            BatchRun batchRun,
            long gProductManagementId,
            IEnumerable<ClProductAttr> attrs,
            IReadOnlyDictionary<string, AttributeDefinition> definitionMap,
            CancellationToken cancellationToken)
        {
            // ステップ1: 既存のEAVデータを全件取得
            var existingEavMap = await _productManagementRepository.GetProductManagementEavMapAsync(
                connection,
                gProductManagementId,
                tx,
                cancellationToken);

            // 未処理キーの集合（最後に is_active=FALSE にするため）
            var untouchedKeys = new HashSet<(string, short)>(existingEavMap.Keys);

            // 統計用カウンタ
            int insertCount = 0;  // 新規作成した件数
            int updateCount = 0;  // 更新した件数
            int skipCount = 0;    // スキップした件数（差分なし）

            Logger.Info($"製品EAV UPSERT開始: g_product_management_id={gProductManagementId}, 既存EAV件数={existingEavMap.Count}, 入力属性件数={attrs.Count()}");

            // ステップ2: 入力属性を1つずつ処理
            foreach (var attr in attrs)
            {
                // チェック1: 属性定義が存在するか
                if (!definitionMap.TryGetValue(attr.AttrCd, out var definition))
                {
                    Logger.Warn($"製品EAVスキップ(定義なし): g_product_management_id={gProductManagementId}, attr_cd={attr.AttrCd}");
                    continue; // 次の属性へ
                }

                // チェック2: is_golden_eav = TRUE かチェック
                // 注意：is_golden_eav = FALSE の属性は製品EAVに書き込まない
                if (!definition.IsGoldenAttrEav)
                {
                    Logger.Info($"製品EAV対象外: g_product_management_id={gProductManagementId}, attr_cd={attr.AttrCd}, is_golden_attr_eav={definition.IsGoldenAttrEav}");
                    continue; // 次の属性へ
                }

                var attrSeq = attr.AttrSeq > 0 ? attr.AttrSeq : (short)1;
                var key = (attr.AttrCd, attrSeq);
                untouchedKeys.Remove(key);

                var provJson = BuildProductManagementEavProvenance(batchRun, attr);
                var payload = new EavPayload(definition.DataType, attr, definition.ProductUnitCd, provJson, null);
                var tempEntity = payload.ToEntity(gProductManagementId, attrSeq);

                var valueText = tempEntity.ValueText;
                var valueNum = tempEntity.ValueNum;
                var valueDate = tempEntity.ValueDate;
                var valueCd = tempEntity.ValueCd;

                if (existingEavMap.TryGetValue(key, out var existing))
                {
                    // 差分チェックして更新
                    var updateValues = new Dictionary<string, object?>();

                    if (!string.Equals(existing.ValueText, valueText, StringComparison.Ordinal))
                        updateValues["value_text"] = valueText;

                    if (existing.ValueNum != valueNum)
                        updateValues["value_num"] = valueNum;

                    if (existing.ValueDate != valueDate)
                        updateValues["value_date"] = valueDate;

                    if (!string.Equals(existing.ValueCd, valueCd, StringComparison.Ordinal))
                        updateValues["value_cd"] = valueCd;

                    if (!string.Equals(existing.UnitCd, definition.ProductUnitCd, StringComparison.Ordinal))
                        updateValues["unit_cd"] = definition.ProductUnitCd;

                    if (!string.Equals(existing.QualityStatus, attr.QualityStatus, StringComparison.Ordinal))
                        updateValues["quality_status"] = attr.QualityStatus;

                    if (!string.Equals(existing.QualityDetailJson, attr.QualityDetailJson, StringComparison.Ordinal))
                        updateValues["quality_detail_json"] = attr.QualityDetailJson;

                    updateValues["provenance_json"] = BuildProductManagementEavProvenance(batchRun, attr);
                    updateValues["batch_id"] = batchRun.BatchId;

                    if (!existing.IsActive)
                        updateValues["is_active"] = true;

                    if (updateValues.Any())
                    {
                        await _productManagementRepository.UpdateProductManagementEavAsync(
                            connection,
                            gProductManagementId,
                            attr.AttrCd,
                            attrSeq,
                            updateValues,
                            tx,
                            cancellationToken);
                        updateCount++;
                    }
                    else
                    {
                        skipCount++;
                    }
                }
                else
                {
                    // 新規作成
                    var newEav = new MProductManagementEav
                    {
                        GProductManagementId = gProductManagementId,
                        AttrCd = attr.AttrCd,
                        AttrSeq = attrSeq,
                        ValueText = valueText,
                        ValueNum = valueNum,
                        ValueDate = valueDate,
                        ValueCd = valueCd,
                        UnitCd = definition.ProductUnitCd,
                        QualityStatus = attr.QualityStatus,
                        QualityDetailJson = attr.QualityDetailJson,
                        ProvenanceJson = BuildProductManagementEavProvenance(batchRun, attr),
                        BatchId = batchRun.BatchId,
                        IsActive = true
                    };

                    await _productManagementRepository.InsertProductManagementEavAsync(
                        connection,
                        newEav,
                        tx,
                        cancellationToken);
                    insertCount++;
                }
            }

            // 未出現の属性を is_active=false にマーク
            int deactivatedCount = 0;
            foreach (var key in untouchedKeys)
            {
                await _productManagementRepository.MarkProductManagementEavInactiveAsync(
                    connection,
                    gProductManagementId,
                    key.Item1,
                    key.Item2,
                    tx,
                    cancellationToken);
                deactivatedCount++;
            }

            Logger.Info($"製品EAV UPSERT完了: g_product_management_id={gProductManagementId}, INSERT={insertCount}, UPDATE={updateCount}, SKIP={skipCount}, DEACTIVATED={deactivatedCount}");
        }

        private static string? GetCatalogDesc(IEnumerable<ClProductAttr> attrs)
        {
            var catalogDescAttr = attrs.FirstOrDefault(a => string.Equals(a.AttrCd, "CATALOG_DESC", StringComparison.OrdinalIgnoreCase));
            return catalogDescAttr?.ValueText;
        }

        private static string BuildProductManagementProvenance(BatchRun batchRun, string sourceProductManagementCd)
        {
            var prov = new
            {
                source_system = batchRun.GroupCompanyCd,
                ingest_profile = $"{batchRun.GroupCompanyCd}_{batchRun.DataKind}",
                idem_key = $"{batchRun.BatchId}:{batchRun.GroupCompanyCd}:{sourceProductManagementCd}",
                rule_version = "v2025.11.09"
            };

            return JsonSerializer.Serialize(prov, JsonOptions);
        }

        private static string BuildProductManagementEavProvenance(BatchRun batchRun, ClProductAttr attr)
        {
            var prov = new
            {
                source_system = batchRun.GroupCompanyCd,
                ingest_profile = $"{batchRun.GroupCompanyCd}_{batchRun.DataKind}",
                idem_key = $"{batchRun.BatchId}:{batchRun.GroupCompanyCd}:{attr.AttrCd}:{attr.AttrSeq}",
                rule_version = attr.RuleVersion ?? "v2025.11.09"
            };

            return JsonSerializer.Serialize(prov, JsonOptions);
        }

        private readonly record struct EnsureIdentResult(long GProductId, bool IsNew);
    }
}






