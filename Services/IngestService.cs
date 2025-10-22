using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;
using System.Text;
using System.Text.Json;
using ProductDataIngestion.Models;
using ProductDataIngestion.Repositories;
using ProductDataIngestion.Repositories.Interfaces;
using Npgsql;
using Dapper;
using System.Reflection;

namespace ProductDataIngestion.Services
{
    /// <summary>
    /// CSVファイル取込サービス (リファクタリング版)
    /// ビジネスフロー: CSV受領 → ルール取得 → 読込・変換 → temp保存 → EAV生成
    /// </summary>
    public class IngestService
    {
        private readonly DataImportService _dataService;
        private readonly IBatchRepository _batchRepository;
        private readonly IProductRepository _productRepository;
        private readonly IDataImportRepository _dataRepository;
        private readonly CsvValidator _csvValidator;
        private readonly AttributeProcessor _attributeProcessor;
        private readonly string _connectionString;

        // 処理中データ保持
        private readonly List<BatchRun> _batchRuns = new();
        private readonly List<TempProductParsed> _tempProducts = new();
        private readonly List<RecordError> _recordErrors = new();

        public IngestService(
            string connectionString,
            IBatchRepository batchRepository,
            IProductRepository productRepository)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _batchRepository = batchRepository ?? throw new ArgumentNullException(nameof(batchRepository));
            _productRepository = productRepository ?? throw new ArgumentNullException(nameof(productRepository));
            _dataService = new DataImportService(_dataRepository);
            _csvValidator = new CsvValidator();
            _attributeProcessor = new AttributeProcessor(_dataService);
        }

        /// <summary>
        /// CSVファイル取込メイン処理
        /// フロー全体: 1.バッチ起票 → 2.ルール取得 → 3-6.CSV処理 → 7-9.EAV生成 → 10.統計更新
        /// </summary>
        public async Task<string> ProcessCsvFileAsync(string filePath, string groupCompanyCd, string targetEntity)
        {
            Console.WriteLine($"=== CSV取込開始 ===\nファイル: {filePath}\nGP会社: {groupCompanyCd}\n処理モード: {targetEntity}");

            // 会社コード検証
            await ValidateCompanyCodeAsync(groupCompanyCd);

            // フロー1: バッチ起票
            var batchId = await CreateBatchRunAsync(filePath, groupCompanyCd, targetEntity);

            try
            {
                // フロー2: ファイル取込ルール取得
                var (importSetting, importDetails) = await FetchImportRulesAsync(groupCompanyCd, targetEntity);

                // フロー3: CSV読み込み前のI/O設定
                var (config, headerRowIndex) = ConfigureCsvReaderSettings(importSetting);

                // フロー4-6: CSV 1行ずつ読込 → 必須チェック → temp保存
                var result = await ReadCsvAndSaveToTempAsync(filePath, batchId, groupCompanyCd,
                                                             headerRowIndex, importDetails, config);

                // フロー7-9: extras_jsonからデータ取得 → 属性生成 → cl_product_attr保存
                await GenerateProductAttributesAsync(batchId, groupCompanyCd, targetEntity);

                // フロー10: バッチ統計更新
                await UpdateBatchStatisticsAsync(batchId, result);

                Console.WriteLine($"=== 取込完了 ===\n読込: {result.readCount}\n成功: {result.okCount}\n失敗: {result.ngCount}");
                return batchId;
            }
            catch (IngestException ex)
            {
                // IngestException は設計書に基づいたエラー
                await RecordErrorAndMarkBatchFailed(batchId, ex);
                throw; // フレームワークに例外を伝播
            }
            catch (Exception ex)
            {
                // 予期しないエラーは DB_ERROR として記録
                var dbError = new IngestException(
                    ErrorCodes.DB_ERROR,
                    $"予期しないエラーが発生しました: {ex.Message}",
                    ex
                );
                await RecordErrorAndMarkBatchFailed(batchId, dbError);
                throw dbError; // フレームワークに例外を伝播
            }
        }

        #region フロー1: バッチ起票

        /// <summary>
        /// フロー1: バッチ起票
        /// - batch_id 採番
        /// - batch_run に idem_key で冪等化レコード作成 (RUNNING)
        /// - started_at = now()
        /// </summary>
        private async Task<string> CreateBatchRunAsync(string filePath, string groupCompanyCd, string targetEntity)
        {
            try
            {
                // batch_id 採番
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

                Console.WriteLine($"バッチ起票完了: {batchId}");
                return batchId;
            }
            catch (Exception ex)
            {
                throw new IngestException(
                    ErrorCodes.DB_ERROR,
                    $"バッチ起票に失敗しました: {ex.Message}",
                    ex
                );
            }
        }

        #endregion

        #region フロー2: ファイル取込ルールの取得

        /// <summary>
        /// フロー2: ファイル取込ルールの取得
        /// - 入力: group_company_cd と target_entity
        /// - m_data_import_setting を探索して有効な profile_id を決定
        /// - 同 profile_id で m_data_import_d を全件取得
        /// - ルール不在/重複は致命的エラー → FAILED
        /// </summary>
        private async Task<(MDataImportSetting, List<MDataImportD>)> FetchImportRulesAsync(
            string groupCompanyCd, string targetEntity)
        {
            try
            {
                string usageNm = $"{groupCompanyCd}-{targetEntity}";
                var importSetting = await _dataService.GetImportSettingAsync(groupCompanyCd, usageNm);

                // is_active チェック
                if (importSetting == null || !importSetting.IsActive)
                {
                    throw new IngestException(
                        ErrorCodes.MAPPING_NOT_FOUND,
                        $"有効なファイル取込設定が見つかりません: {usageNm}"
                    );
                }

                // 列マッピング取得
                var importDetails = await _dataService.GetImportDetailsAsync(importSetting.ProfileId);
                Console.WriteLine($"取込ルール取得完了: ProfileId={importSetting.ProfileId}, 列数={importDetails.Count}");

                return (importSetting, importDetails);
            }
            catch (IngestException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new IngestException(
                    ErrorCodes.DB_ERROR,
                    $"取込ルール取得に失敗しました: {ex.Message}",
                    ex
                );
            }
        }

        #endregion

        #region フロー3: CSV読み込み前のI/O設定

        /// <summary>
        /// フロー3: CSV読み込み前のI/O設定
        /// - 文字コード、区切り、ヘッダ行スキップを設定
        /// - header_row_index で指定された行をヘッダーとして読み込み、その後のデータ行のみ処理
        /// </summary>
        private (CsvConfiguration, int) ConfigureCsvReaderSettings(MDataImportSetting importSetting)
        {
            try
            {
                var config = new CsvConfiguration(CultureInfo.InvariantCulture)
                {
                    HasHeaderRecord = true,
                    Delimiter = importSetting.Delimiter ?? ",",
                    BadDataFound = context => { },
                    MissingFieldFound = null,
                    Encoding = GetEncodingFromCharacterCode(importSetting.CharacterCd ?? "UTF-8")
                };

                return (config, importSetting.HeaderRowIndex);
            }
            catch (Exception ex)
            {
                throw new IngestException(
                    ErrorCodes.INVALID_ENCODING,
                    $"CSV設定の初期化に失敗しました: {ex.Message}",
                    ex
                );
            }
        }

        #endregion

        #region フロー4-6: CSV読込 → 必須チェック → temp保存

        /// <summary>
        /// フロー4-6: CSV読込 → 必須チェック → temp保存
        /// - header_row_index で指定された行までスキップし、その行をヘッダーとして読み込む
        /// - その後のデータ行を処理 (変換はConfigで既に設定済み)
        /// - column_seq = 0: 公司コード注入
        /// - column_seq > 0: CSV列番号 (そのままCSV配列インデックスとして使用)
        /// </summary>
        private async Task<(int readCount, int okCount, int ngCount)> ReadCsvAndSaveToTempAsync(
            string filePath, string batchId, string groupCompanyCd,
            int headerRowIndex, List<MDataImportD> importDetails, CsvConfiguration config)
        {
            int readCount = 0, okCount = 0, ngCount = 0;

            try
            {
                using var reader = new StreamReader(filePath, config.Encoding ?? Encoding.UTF8);
                using var csv = new CsvReader(reader, config);

                // フロー4: ヘッダー行までスキップ
                for (int i = 0; i < headerRowIndex - 1; i++)
                {
                    if (!await csv.ReadAsync())
                    {
                        throw new IngestException(
                            ErrorCodes.PARSE_FAILED,
                            $"ヘッダー行 {headerRowIndex} まで到達できません"
                        );
                    }
                }

                // ヘッダー行を読み込む
                if (!await csv.ReadAsync())
                {
                    throw new IngestException(
                        ErrorCodes.PARSE_FAILED,
                        "ヘッダー行が読み込めません"
                    );
                }

                csv.ReadHeader();
                var headers = csv.HeaderRecord;
                if (headers == null || headers.Length == 0)
                {
                    throw new IngestException(
                        ErrorCodes.PARSE_FAILED,
                        "ヘッダー行が空です"
                    );
                }

                Console.WriteLine($"ヘッダー取得完了: {headers.Length} 列");

                // 列マッピング検証 (CsvValidatorを使用)
                _csvValidator.ValidateColumnMappings(importDetails, headers);

                // データ行処理開始
                long dataRowNumber = 0;
                int currentPhysicalLine = headerRowIndex;

                while (await csv.ReadAsync())
                {
                    currentPhysicalLine++;
                    dataRowNumber++;
                    readCount++;

                    var record = csv.Parser.Record;

                    try
                    {
                        // 空レコード検証 (CsvValidatorを使用)
                        _csvValidator.ValidateEmptyRecord(record, dataRowNumber, currentPhysicalLine);

                        // CSV行をtempProductにマッピング
                        MapCsvRowToTempProduct(batchId, groupCompanyCd, dataRowNumber, currentPhysicalLine,
                                               record!, headers, importDetails);
                        okCount++;
                    }
                    catch (IngestException ex)
                    {
                        RecordIngestError(batchId, dataRowNumber, currentPhysicalLine, ex, record);
                        ngCount++;
                    }
                    catch (Exception ex)
                    {
                        var ingestEx = new IngestException(
                            ErrorCodes.PARSE_FAILED,
                            $"行の処理中にエラーが発生しました: {ex.Message}",
                            ex,
                            recordRef: $"line:{dataRowNumber}"
                        );
                        RecordIngestError(batchId, dataRowNumber, currentPhysicalLine, ingestEx, record);
                        ngCount++;
                    }
                }

                // データベース保存 (フロー6: temp への保存)
                await SaveToTempTablesAsync();

                return (readCount, okCount, ngCount);
            }
            catch (IngestException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new IngestException(
                    ErrorCodes.PARSE_FAILED,
                    $"CSV読み込み中にエラーが発生しました: {ex.Message}",
                    ex
                );
            }
        }

        /// <summary>
        /// フロー4-5: CSV行をtempProductにマッピング + 必須チェック
        /// - column_seq = 0: 公司コード注入
        /// - column_seq > 0: CSV列番号 (1始まり、配列インデックスは -1 が必要)
        /// - transform_expr 適用 (trim(@))
        /// - is_required チェック
        /// - CSV範囲外の列はスキップ (オプション列のみ)
        /// </summary>
        private void MapCsvRowToTempProduct(
            string batchId, string groupCompanyCd, long dataRowNumber, int currentPhysicalLine,
            string[] record, string[] headers, List<MDataImportD> importDetails)
        {
            var tempProduct = new TempProductParsed
            {
                TempRowId = Guid.NewGuid(),
                BatchId = batchId,
                LineNo = dataRowNumber,
                SourceGroupCompanyCd = groupCompanyCd,
                StepStatus = "READY",
                ExtrasJson = "{}"
            };

            var extrasDict = new Dictionary<string, object>();
            var sourceRawDict = new Dictionary<string, string>();
            var requiredFieldErrors = new List<string>();

            // 列ごとのルールグループ化（column_seq 単位）
            var groupedDetails = importDetails
                .OrderBy(d => d.ColumnSeq)
                .ThenBy(d => d.ProjectionKind)  // PRODUCT優先（安定化）
                .GroupBy(d => d.ColumnSeq)
                .ToDictionary(g => g.Key, g => g.ToList());

            // すべての列を処理して extras_json に保存
            // （is_required に関わらず、PRODUCT と PRODUCT_EAV の両方）
            foreach (var kvp in groupedDetails)
            {
                int colSeq = kvp.Key;
                var detailsForCol = kvp.Value;  // 該列の全ルール（多条可）
                string? rawValue = null;
                string headerName = "N/A";
                bool isInjectedValue = (colSeq == 0);

                if (colSeq == 0)
                {
                    // 注入列: 会社コード固定値
                    rawValue = groupCompanyCd;
                    headerName = "[注入:group_company_cd]";
                }
                else
                {
                    // CSV列: 範囲チェック
                    int csvIndex = colSeq - 1;
                    if (csvIndex >= headers.Length || csvIndex >= record.Length)
                    {
                        continue;  // 範囲外スキップ
                    }
                    rawValue = record[csvIndex];
                    headerName = headers[csvIndex];

                    // 生値バックアップ（CSVのみ）
                    string backupKey = headerName;
                    sourceRawDict[backupKey] = rawValue ?? "";
                }

                // 無ルール: エラー記録
                if (!detailsForCol.Any())
                {   
                    var profileId = importDetails.FirstOrDefault()?.ProfileId ?? 0L;  // デフォルト0（エラー時）
                    var noRuleEx = new IngestException(
                        ErrorCodes.MAPPING_NOT_FOUND,
                        $"列 {colSeq} の取込ルールが見つかりません（profile_id: {profileId}）",
                        recordRef: $"column:{colSeq}"
                    );
                    RecordIngestError(batchId, dataRowNumber, currentPhysicalLine, noRuleEx, record);
                    continue;
                }

               // 全ルール処理（CSV値共有、変換独立）
                int subIndex = 0;
                foreach (var detail in detailsForCol)
                {
                    string? transformedValue = ApplyTransformExpression(rawValue, detail.TransformExpr ?? "");

                    // FIXEDフィールド反映（PRODUCT限定）
                    bool mappingSuccess = false;
                    if (!string.IsNullOrEmpty(detail.TargetColumn) &&
                        detail.ProjectionKind == "PRODUCT"&& 
                        detail.IsRequired)  
                    {
                        string propertyName = "source" + ConvertToPascalCase(detail.TargetColumn);
                        mappingSuccess = SetTempProductProperty(tempProduct, propertyName, transformedValue);
                    }

                    // extras_jsonに保存（ユニーク化:col_XX_ATTR_CD）
                    string uniqueKey = string.IsNullOrEmpty(detail.AttrCd) 
                        ? $"col_{colSeq}_sub{subIndex++}"  // AttrCd空時、数字を利用する
                        : $"col_{colSeq}_{detail.AttrCd.Replace(":", "_")}";  // e.g., "col_0_GROUP_COMPANY_CD"
                    extrasDict[uniqueKey] = new
                    {
                        csv_column_index = colSeq,
                        header = headerName,
                        raw_value = rawValue ?? "",
                        transformed_value = transformedValue ?? "",
                        target_column = detail.TargetColumn ?? "",
                        projection_kind = detail.ProjectionKind,
                        attr_cd = detail.AttrCd ?? "",
                        transform_expr = detail.TransformExpr ?? "",
                        is_required = detail.IsRequired,
                        is_injected = false,
                        mapping_success = mappingSuccess
                    };

                    // 必須チェック（true時のみ）
                    if (detail.IsRequired && string.IsNullOrWhiteSpace(transformedValue))
                    {
                        requiredFieldErrors.Add($"列 {colSeq} ({detail.AttrCd}): 必須項目空");
                    }
                }
            }

            // 必須チェック結果 (CsvValidatorを使用)
            _csvValidator.ValidateRequiredFields(requiredFieldErrors, dataRowNumber, currentPhysicalLine);

            // extras_json 最終化
            tempProduct.ExtrasJson = JsonSerializer.Serialize(new
            {
                source_raw = sourceRawDict,
                processed_columns = extrasDict,
                csv_headers = headers,
                physical_line = currentPhysicalLine,
                data_row_number = dataRowNumber,
                processing_timestamp = DateTime.UtcNow
            }, new JsonSerializerOptions { WriteIndented = false });

            _tempProducts.Add(tempProduct);
        }

        #endregion

        #region フロー7-9: extras_jsonからデータ取得 → 属性生成 → cl_product_attr保存

        /// <summary>
        /// フロー7-9: 属性マッピングと cl_product_attr 作成
        /// 新仕様: extras_jsonからデータを取得して処理
        /// - PRODUCT と PRODUCT_EAV の両方を処理
        /// - value_role: ID_AND_LABEL, ID_ONLY, LABEL_ONLY に対応
        /// </summary>
        private async Task GenerateProductAttributesAsync(
            string batchId, string groupCompanyCd, string dataKind)
        {
            try
            {
                //全てのtempProductを処理して、cl_product_attrを生成
                var allProductAttrs = new List<ClProductAttr>();

                foreach (var tempProduct in _tempProducts)
                {
                    // AttributeProcessorを使用して属性を処理
                    var productAttrs = await _attributeProcessor.ProcessAttributesAsync(
                        batchId,
                        tempProduct,
                        groupCompanyCd,
                        dataKind
                    );

                    allProductAttrs.AddRange(productAttrs);
                }

                // データベース保存
                await _productRepository.SaveProductAttributesAsync(allProductAttrs);
                Console.WriteLine($"cl_product_attr保存完了: {allProductAttrs.Count} レコード");
            }
            catch (IngestException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new IngestException(
                    ErrorCodes.DB_ERROR,
                    $"属性生成中にエラーが発生しました: {ex.Message}",
                    ex
                );
            }
        }

        #endregion

        #region フロー10: バッチ統計更新

        /// <summary>
        /// フロー10: バッチ統計更新
        /// - batch_run.counts_json の read/ok/ng 更新
        /// - batch_status を SUCCESS or PARTIAL に更新
        /// - ended_at = now()
        /// </summary>
        private async Task UpdateBatchStatisticsAsync(string batchId, (int readCount, int okCount, int ngCount) result)
        {
            try
            {
                var batchRun = _batchRuns.FirstOrDefault(b => b.BatchId == batchId);
                if (batchRun != null)
                {
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
                    Console.WriteLine($"バッチ統計更新完了: {batchRun.BatchStatus}");
                }
            }
            catch (Exception ex)
            {
                throw new IngestException(
                    ErrorCodes.DB_ERROR,
                    $"バッチ統計更新に失敗しました: {ex.Message}",
                    ex
                );
            }
        }

        #endregion

        #region エラー記録・バッチ失敗処理

        /// <summary>
        /// Ingestエラーレコード記録
        /// </summary>
        private void RecordIngestError(string batchId, long dataRowNumber, int currentPhysicalLine,
                                IngestException ex, string[]? record)
        {
            var error = new RecordError
            {
                BatchId = batchId,
                Step = "INGEST",
                RecordRef = !string.IsNullOrEmpty(ex.RecordRef) ? ex.RecordRef : $"line:{dataRowNumber}",
                ErrorCd = ex.ErrorCode,
                ErrorDetail = $"データ行 {dataRowNumber} (物理行 {currentPhysicalLine}): {ex.Message}",
                RawFragment = !string.IsNullOrEmpty(ex.RawFragment)
                    ? ex.RawFragment
                    : string.Join(",", record?.Take(5) ?? Array.Empty<string>())
            };

            Console.WriteLine($"エラーレコード: [{error.ErrorCd}] {error.ErrorDetail}");
            _recordErrors.Add(error);
        }

        /// <summary>
        /// エラー記録 + バッチ失敗マーク
        /// </summary>
        private async Task RecordErrorAndMarkBatchFailed(string batchId, IngestException ex)
        {
            // エラーをrecord_errorテーブルに記録
            var error = new RecordError
            {
                BatchId = batchId,
                Step = "INGEST",
                RecordRef = ex.RecordRef,
                ErrorCd = ex.ErrorCode,
                ErrorDetail = ex.Message,
                RawFragment = ex.RawFragment
            };

            _recordErrors.Add(error);
            await _productRepository.SaveRecordErrorsAsync(_recordErrors);

            // バッチを失敗としてマーク
            await MarkBatchAsFailedAsync(batchId, ex.Message);
        }

        /// <summary>
        /// バッチ失敗マーク
        /// </summary>
        private async Task MarkBatchAsFailedAsync(string batchId, string errorMessage)
        {
            try
            {
                var batchRun = _batchRuns.FirstOrDefault(b => b.BatchId == batchId);
                if (batchRun != null)
                {
                    batchRun.BatchStatus = "FAILED";
                    batchRun.EndedAt = DateTime.UtcNow;
                    await _batchRepository.UpdateBatchRunAsync(batchRun);
                    Console.WriteLine($"バッチ失敗: {errorMessage}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"バッチ失敗マーク中にエラー: {ex.Message}");
            }
        }

        #endregion

        #region データベース保存 (Repository 経由)

        /// <summary>
        /// フロー6: temp への保存 (Repository 経由)
        /// </summary>
        private async Task SaveToTempTablesAsync()
        {
            try
            {
                await _productRepository.SaveTempProductsAsync(_tempProducts);
                await _productRepository.SaveRecordErrorsAsync(_recordErrors);

                Console.WriteLine($"temp保存完了: 商品={_tempProducts.Count}, エラー={_recordErrors.Count}");
            }
            catch (Exception ex)
            {
                throw new IngestException(
                    ErrorCodes.DB_ERROR,
                    $"temp保存に失敗しました: {ex.Message}",
                    ex
                );
            }
        }

        #endregion

        #region ヘルパーメソッド

        /// <summary>
        /// transform_expr 適用
        /// 対応する変換:
        /// - trim(@): 前後の半角・全角スペース削除
        /// - upper(@): 大文字変換
        /// - nullif(@,''): 空文字→null
        /// - to_timestamp(@,'YYYY-MM-DD'): 日付変換
        /// </summary>
        private string? ApplyTransformExpression(string? value, string transformExpr)
        {
            // null 入力はそのまま返す
            if (value == null) return null;

            string? result = value;

            // transform_expr が空の場合はデフォルトの trim のみ適用
            if (string.IsNullOrEmpty(transformExpr))
            {
                return value.Trim().Trim('\u3000'); // 半角・全角スペース削除
            }

            // 複数の変換を順次適用（パイプライン処理）
            // 例: "trim(@),upper(@)" → trim を適用してから upper を適用
            var transformations = transformExpr.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries);

            foreach (var transformation in transformations)
            {
                var expr = transformation.Trim();

                // 1. trim(@): 前後の半角・全角スペース削除
                if (expr.Equals("trim(@)", StringComparison.OrdinalIgnoreCase))
                {
                    if (result != null)
                    {
                        result = result.Trim().Trim('\u3000'); // 半角スペース + 全角スペース削除
                    }
                }
                // 2. upper(@): 大文字変換
                else if (expr.Equals("upper(@)", StringComparison.OrdinalIgnoreCase))
                {
                    if (result != null)
                    {
                        result = result.ToUpper(); // すべての文字を大文字に
                    }
                }
                // 3. nullif(@,''): 空文字→null
                else if (expr.StartsWith("nullif(@", StringComparison.OrdinalIgnoreCase))
                {
                    if (string.IsNullOrWhiteSpace(result))
                    {
                        result = null; // 空文字または空白のみの場合は null に変換
                    }
                }
                // 4. to_timestamp(@,'YYYY-MM-DD'): 日付変換
                else if (expr.StartsWith("to_timestamp(@", StringComparison.OrdinalIgnoreCase))
                {
                    if (!string.IsNullOrWhiteSpace(result))
                    {
                        result = ParseDateExpression(result, expr); // 日付パース処理
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// 日付変換処理
        /// to_timestamp(@,'YYYY-MM-DD') などのフォーマット指定に対応
        /// パース失敗時は元の値をそのまま返す（例外を投げない）
        /// </summary>
        private string? ParseDateExpression(string value, string expression)
        {
            try
            {
                // 正規表現でフォーマット文字列を抽出
                // 例: "to_timestamp(@,'YYYY-MM-DD')" → "YYYY-MM-DD"
                var match = System.Text.RegularExpressions.Regex.Match(
                    expression,
                    @"to_timestamp\(@\s*,\s*'([^']+)'\)",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase
                );

                if (!match.Success)
                {
                    // フォーマット文字列が見つからない場合は警告を出力
                    Console.WriteLine($"[警告] 日付フォーマット解析失敗: {expression}");
                    return value; // 元の値を返す
                }

                var formatPattern = match.Groups[1].Value; // 例: "YYYY-MM-DD"

                // PostgreSQL形式 → .NET形式へ変換
                // 例: "YYYY-MM-DD" → "yyyy-MM-dd"
                var dotNetFormat = ConvertPostgreSqlFormatToDotNet(formatPattern);

                // DateOnly でパース試行 (日付のみの場合)
                if (DateOnly.TryParseExact(value.Trim(), dotNetFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dateOnly))
                {
                    // ISO 8601形式 (YYYY-MM-DD) で返す
                    return dateOnly.ToString("yyyy-MM-dd");
                }
                // DateTime でパース試行 (日時が含まれる場合)
                else if (DateTime.TryParseExact(value.Trim(), dotNetFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dateTime))
                {
                    // 日付部分のみを ISO 8601形式で返す
                    return dateTime.ToString("yyyy-MM-dd");
                }
                else
                {
                    // パース失敗時は警告を出力し、元の値を返す
                    Console.WriteLine($"[警告] 日付パース失敗: value='{value}', format='{dotNetFormat}'");
                    return value;
                }
            }
            catch (Exception ex)
            {
                // 予期しないエラーが発生した場合
                Console.WriteLine($"[エラー] 日付変換エラー: {ex.Message}");
                return value; // 元の値を返す
            }
        }

        /// <summary>
        /// PostgreSQL日付フォーマット → .NET日付フォーマット変換
        /// 例: "YYYY-MM-DD" → "yyyy-MM-dd"
        /// </summary>
        private string ConvertPostgreSqlFormatToDotNet(string pgFormat)
        {
            // PostgreSQL形式 → .NET形式のマッピングテーブル
            // 例: YYYY (PostgreSQL) → yyyy (.NET)
            var conversions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                { "YYYY", "yyyy" },  // 4桁年
                { "YY", "yy" },      // 2桁年
                { "MM", "MM" },      // 月
                { "DD", "dd" },      // 日
                { "HH24", "HH" },    // 24時間制の時
                { "HH12", "hh" },    // 12時間制の時
                { "MI", "mm" },      // 分
                { "SS", "ss" },      // 秒
                { "MS", "fff" }      // ミリ秒
            };

            string result = pgFormat;

            // 各変換ルールを順次適用
            foreach (var kvp in conversions)
            {
                // 正規表現で大文字小文字を区別せずに置換
                result = System.Text.RegularExpressions.Regex.Replace(
                    result,
                    kvp.Key,              // PostgreSQL形式 (例: "YYYY")
                    kvp.Value,            // .NET形式 (例: "yyyy")
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase
                );
            }

            return result; // 変換後のフォーマット文字列を返す
        }

        /// <summary>
        /// TempProductParsed プロパティ値設定
        /// </summary>
        private bool SetTempProductProperty(TempProductParsed obj, string propertyName, string? value)
        {
            try
            {
                var property = typeof(TempProductParsed).GetProperty(propertyName);
                if (property != null && property.CanWrite)
                {
                    property.SetValue(obj, value);
                    return true;
                }

                property = typeof(TempProductParsed).GetProperty(propertyName,
                    BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);

                if (property != null && property.CanWrite)
                {
                    property.SetValue(obj, value);
                    return true;
                }

                return false;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// snake_case → PascalCase 変換
        /// </summary>
        private string ConvertToPascalCase(string input)
        {
            if (string.IsNullOrEmpty(input)) return input;

            var parts = input.Split(new char[] { '_', '-' }, StringSplitOptions.RemoveEmptyEntries);
            return string.Concat(parts.Select(part =>
                char.ToUpperInvariant(part[0]) + part.Substring(1).ToLowerInvariant()));
        }

        /// <summary>
        /// GP会社コード検証
        /// </summary>
        private async Task ValidateCompanyCodeAsync(string groupCompanyCd)
        {
            if (string.IsNullOrWhiteSpace(groupCompanyCd))
            {
                throw new IngestException(
                    ErrorCodes.MISSING_COLUMN,
                    "GP会社コードが指定されていません"
                );
            }

            try
            {
                using var connection = new NpgsqlConnection(_connectionString);
                await connection.OpenAsync();

                var sql = @"
                    SELECT group_company_id as GroupCompanyId, group_company_cd as GroupCompanyCd,
                           group_company_nm as GroupCompanyNm, default_currency_cd as DefaultCurrencyCd,
                           is_active as IsActive, cre_at as CreAt, upd_at as UpdAt
                    FROM m_company
                    WHERE group_company_cd = @GroupCompanyCd AND is_active = true";

                var company = await connection.QueryFirstOrDefaultAsync<MCompany>(
                    sql, new { GroupCompanyCd = groupCompanyCd });

                if (company == null)
                {
                    throw new IngestException(
                        ErrorCodes.MAPPING_NOT_FOUND,
                        $"GP会社コードが存在しないか無効です: {groupCompanyCd}"
                    );
                }

                if (!company.IsValid())
                {
                    throw new IngestException(
                        ErrorCodes.MAPPING_NOT_FOUND,
                        $"GP会社コードのデータが無効です: {groupCompanyCd}"
                    );
                }

                Console.WriteLine($"GP会社検証成功: {company.GroupCompanyCd} - {company.GroupCompanyNm}");
            }
            catch (Npgsql.PostgresException ex) when (ex.SqlState == "42P01")
            {
                await ValidateCompanyCodeSimpleAsync(groupCompanyCd);
            }
            catch (IngestException)
            {
                throw;
            }
            catch (Exception)
            {
                await ValidateCompanyCodeSimpleAsync(groupCompanyCd);
            }
        }

        /// <summary>
        /// GP会社コード簡易検証
        /// </summary>
        private async Task ValidateCompanyCodeSimpleAsync(string groupCompanyCd)
        {
            var validCompanyCodes = new[] { "KM", "RKE", "KBO" };

            if (!validCompanyCodes.Contains(groupCompanyCd.ToUpper()))
            {
                throw new IngestException(
                    ErrorCodes.MAPPING_NOT_FOUND,
                    $"GP会社コードが認識されません: {groupCompanyCd}"
                );
            }

            Console.WriteLine($"GP会社コード簡易検証: {groupCompanyCd}");
            await Task.CompletedTask;
        }

        /// <summary>
        /// 文字コード取得
        /// </summary>
        private Encoding GetEncodingFromCharacterCode(string characterCd)
        {
            return characterCd?.ToUpperInvariant() switch
            {
                "UTF-8" => Encoding.UTF8,
                "SHIFT_JIS" => Encoding.GetEncoding("Shift_JIS"),
                "EUC-JP" => Encoding.GetEncoding("EUC-JP"),
                _ => Encoding.UTF8
            };
        }

        #endregion
    }
}
