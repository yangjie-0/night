using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;
using System.Text;
using System.Text.Json;
using ProductDataIngestion.Models;
using Npgsql;
using Dapper;
using System.Reflection;
using System.IO;
using System.Collections.Generic;
using System.Linq;

namespace ProductDataIngestion.Services
{
    // CSV文件导入处理服务。
    public class IngestService
    {
        // 数据导入服务实例。
        private readonly DataImportService _dataService;
        // 数据库连接字符串。
        private readonly string _connectionString;
        // 批处理运行记录列表。
        private readonly List<BatchRun> _batchRuns = new();
        // 临时产品解析记录列表。
        private readonly List<TempProductParsed> _tempProducts = new();
        // 产品属性记录列表。
        private readonly List<ClProductAttr> _productAttrs = new();
        // 记录错误列表。
        private readonly List<RecordError> _recordErrors = new();

        // 构造函数：初始化服务。
        public IngestService(string connectionString)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _dataService = new DataImportService(connectionString);
        }

        // GP会社コードの簡易検証（存在チェック、将来はDB照会に置換可能）
        private async Task ValidateCompanyAsync(string groupCompanyCd)
        {
            if (string.IsNullOrWhiteSpace(groupCompanyCd))
            {
                throw new ArgumentException("GP会社コードが指定されていません。", nameof(groupCompanyCd));
            }

            // 現在は簡易検証のみ実施。将来的に _dataService を使った詳細チェックに差し替える。
            Console.WriteLine($"✓ GP会社コード検証: {groupCompanyCd}");

            await Task.CompletedTask;
        }

        // 异步处理CSV文件导入。
        public async Task<string> ProcessCsvFileAsync(string filePath, string groupCompanyCd, string targetEntity = "PRODUCT")
        {
            Console.WriteLine("=== 取込処理開始 ===");
            Console.WriteLine($"ファイルパス: {filePath}");
            Console.WriteLine($"GP会社コード: {groupCompanyCd}");
            Console.WriteLine($"ターゲットエンティティ: {targetEntity}");

            // 0. GP会社コード検証
            await ValidateCompanyAsync(groupCompanyCd);

            // 1. バッチ起票
            var batchId = await Step1_CreateBatchRun(filePath, groupCompanyCd, targetEntity);

            try
            {
                // 2. ファイル取込ルールの取得
                var (importSetting, importDetails) = await Step2_GetImportRules(groupCompanyCd, targetEntity);

                // 3. CSV読み込み前のI/O設定
                var config = Step3_ConfigureCsvReader(importSetting);

                // 4-6. CSV処理とtemp保存（第三步：m_data_import_d列处理逻辑）
                var result = await Step4To6_ProcessCsvAndSaveToTemp(filePath, batchId, groupCompanyCd, importSetting, importDetails, config);

                // 7-9. 属性マッピングとcl_product_attr作成（第三步后续：EAV生成及固定字段转换）
                await Step7To9_CreateProductAttributes(batchId, groupCompanyCd, importDetails);

                // 10. バッチ統計更新
                await Step10_UpdateBatchStatistics(batchId, result);

                Console.WriteLine("\n=== 取込処理完了 ===");
                return batchId;
            }
            catch (Exception ex)
            {
                await MarkBatchAsFailed(batchId, ex.Message);
                throw;
            }
        }

        // 打印导入结果摘要。
        public void PrintResults()
        {
            Console.WriteLine("\n=== 取込結果サマリー ===");
            Console.WriteLine($"TempProductParsed: {_tempProducts.Count}");
            Console.WriteLine($"ClProductAttr: {_productAttrs.Count}");
            Console.WriteLine($"RecordError: {_recordErrors.Count}");

            var stats = _batchRuns.LastOrDefault();
            if (stats != null)
            {
                Console.WriteLine($"最終バッチ状態: {stats.BatchStatus}");
            }
        }

        // ステップ1: バッチ起票。
        private async Task<string> Step1_CreateBatchRun(string filePath, string groupCompanyCd, string targetEntity)
        {
            Console.WriteLine("\n--- ステップ1: バッチ起票 ---");

            // バッチID生成
            string batchId = $"BATCH_{DateTime.Now:yyyyMMddHHmmss}_{Guid.NewGuid():N}";
            Console.WriteLine($"生成 BatchId: {batchId}");

            // IdemKey生成 (S3 key + ETagの代わりにファイルパス+最終更新日時)
            var fileInfo = new FileInfo(filePath);
            string idemKey = $"{filePath}_{fileInfo.LastWriteTime.Ticks}";

            // batch_run 作成
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

            // データベースに保存
            await SaveBatchRunToDatabase(batchRun);

            _batchRuns.Add(batchRun);
            Console.WriteLine($"✓ バッチ起票完了: {batchId}");

            return batchId;
        }

        // ステップ2: ファイル取込ルールの取得（通过GP会社コード + 用途名=GP-PRODUCT确定profile_id）。
        private async Task<(MDataImportSetting, List<MDataImportD>)> Step2_GetImportRules(string groupCompanyCd, string targetEntity)
        {
            Console.WriteLine("\n--- ステップ2: ファイル取込ルールの取得 ---");

            string usageNm = $"{groupCompanyCd}-{targetEntity}"; // 用途名：GP会社コード + PRODUCT
            Console.WriteLine($"探索用途名: {usageNm}");

            // データベースから設定を取得（异步版本）
            var importSetting = await _dataService.GetImportSettingAsync(groupCompanyCd, usageNm);
            
            if (importSetting == null)
            {
                throw new Exception($"ファイル取込設定が見つかりません: GP会社コード={groupCompanyCd}, 用途名={usageNm}");
            }

            // 設定の検証
            if (!importSetting.IsActive)
            {
                throw new Exception($"ファイル取込設定が無効です: ProfileId={importSetting.ProfileId}");
            }

            Console.WriteLine($"✅ プロファイルID: {importSetting.ProfileId}");
            Console.WriteLine($"✅ 用途名: {importSetting.UsageNm}");
            Console.WriteLine($"✅ ターゲット: {importSetting.TargetEntity}");
            Console.WriteLine($"✅ 文字コード: {importSetting.CharacterCd}");
            Console.WriteLine($"✅ 区切り文字: '{importSetting.Delimiter}'");
            Console.WriteLine($"✅ ヘッダー行番号: {importSetting.HeaderRowIndex}");
            Console.WriteLine($"✅ スキップ行数: {importSetting.SkipRowCount}");
            Console.WriteLine($"✅ 有効フラグ: {importSetting.IsActive}");
            
            if (!string.IsNullOrEmpty(importSetting.ImportSettingRemarks))
            {
                Console.WriteLine($"✅ 備考: {importSetting.ImportSettingRemarks}");
            }

            // 列マッピングの取得
            var importDetails = await _dataService.GetImportDetailsAsync(importSetting.ProfileId);
            Console.WriteLine($"✅ 列マッピング数: {importDetails.Count}");

            // 简单输出读取的设定表内容
            Console.WriteLine("\n📋 读取的设定表内容:");
            Console.WriteLine($"  profile_id: {importSetting.ProfileId}");
            Console.WriteLine($"  usage_nm: {importSetting.UsageNm}");
            Console.WriteLine($"  group_company_cd: {importSetting.GroupCompanyCd}");
            Console.WriteLine($"  target_entity: {importSetting.TargetEntity}");
            Console.WriteLine($"  character_cd: {importSetting.CharacterCd}");
            Console.WriteLine($"  delimiter: '{importSetting.Delimiter}'");
            Console.WriteLine($"  header_row_index: {importSetting.HeaderRowIndex}");
            Console.WriteLine($"  skip_row_count: {importSetting.SkipRowCount}");
            Console.WriteLine($"  is_active: {importSetting.IsActive}");
            
            if (!string.IsNullOrEmpty(importSetting.ImportSettingRemarks))
            {
                Console.WriteLine($"  import_setting_remarks: {importSetting.ImportSettingRemarks}");
            }

            return (importSetting, importDetails);
        }

        // ステップ3: CSV読み込み前のI/O設定。
        private CsvConfiguration Step3_ConfigureCsvReader(MDataImportSetting importSetting)
        {
            Console.WriteLine("\n--- ステップ3: CSV読み込み設定 ---");

            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = false, // 手動でヘッダー処理
                Delimiter = importSetting.Delimiter ?? ",",
                BadDataFound = context => 
                {
                    Console.WriteLine($"不良データ検出: {context.RawRecord}");
                },
                MissingFieldFound = null,
                Encoding = GetEncoding(importSetting.CharacterCd ?? "UTF-8")
            };

            Console.WriteLine($"✓ CSV設定完了: 区切り文字='{importSetting.Delimiter}', 文字コード={importSetting.CharacterCd}");

            return config;
        }

        // ステップ4-6: CSV処理とtemp保存（第三步：m_data_import_d列处理逻辑）。
        private async Task<(int readCount, int okCount, int ngCount)> Step4To6_ProcessCsvAndSaveToTemp(
            string filePath, string batchId, string groupCompanyCd,
            MDataImportSetting importSetting, List<MDataImportD> importDetails, CsvConfiguration config)
        {
            Console.WriteLine("\n--- ステップ4-6: CSV読込・変換・必須チェック・temp保存 ---");

            int readCount = 0, okCount = 0, ngCount = 0;

            using var reader = new StreamReader(filePath, config.Encoding ?? Encoding.UTF8);
            using var csv = new CsvReader(reader, config);

            // ヘッダー処理（ヘッダー行跳过）
            string[]? headers = null;
            long currentLine = 0;

            // ヘッダー行まで読み進める（HeaderRowIndex根据数据库读取跳过头行）
            while (currentLine < importSetting.HeaderRowIndex)
            {
                if (await csv.ReadAsync())
                {
                    currentLine++;
                    if (currentLine == importSetting.HeaderRowIndex)
                    {
                        headers = csv.Parser.Record;
                        Console.WriteLine($"✓ ヘッダー行読み込み (行 {currentLine}): {headers?.Length} 列");
                    }
                    else
                    {
                        Console.WriteLine($"⏩ 行 {currentLine} をスキップ (ヘッダー前)");
                    }
                }
                else
                {
                    break;
                }
            }

            // スキップ行の処理（根据数据库skip_row_count解析为特定行号，只跳过指定行）
            var skipRows = ParseSkipRows(importSetting.SkipRows);
            Console.WriteLine($"スキップ対象行: {(skipRows.Any() ? string.Join(", ", skipRows) : "なし")}");

            Console.WriteLine($"\n--- データ行処理開始 ---");

            // データ行処理
            long dataRowNumber = 0; // データ行番号（ヘッダー行以降）

            while (await csv.ReadAsync())
            {
                currentLine++;
                dataRowNumber++;

                // 只跳过指定的那一行（基于数据行号检查）
                if (skipRows.Contains(dataRowNumber))
                {
                    Console.WriteLine($"⏩ データ行 {dataRowNumber} をスキップ (設定によるスキップ)");
                    continue;
                }

                readCount++;

                try
                {
                    // 表示用行番号 (データ行番号を使用)
                    long displayLineNo = dataRowNumber;

                    var tempProduct = new TempProductParsed
                    {
                        TempRowId = Guid.NewGuid(),
                        BatchId = batchId,
                        LineNo = displayLineNo,
                        SourceGroupCompanyCd = groupCompanyCd,
                        StepStatus = "READY",
                        ExtrasJson = "{}"
                    };

                    Console.WriteLine($"\n📝 データ行番号 {displayLineNo} (ファイル行 {currentLine}):");

                    var extrasDict = new Dictionary<string, object>();
                    var sourceRawDict = new Dictionary<string, string>();

                    // 第三步：挨列处理 m_data_import_d（根据target_column/attr_cd决定逻辑）
                    foreach (var detail in importDetails.OrderBy(d => d.ColumnSeq)) // 按列序号排序处理
                    {
                        int colIndex = detail.ColumnSeq; // column_seq 从0开始，CSV Record 从0开始

                        if (colIndex < 0 || colIndex >= (csv.Parser.Record?.Length ?? 0))
                        {
                            Console.WriteLine($"  列{detail.ColumnSeq}: [範囲外]");
                            continue;
                        }

                        string? rawValue = csv.Parser.Record[colIndex]; // 直接从Record获取raw值
                        
                        // ステップ4: 変換適用（trim等）
                        string? transformedValue = ApplyTransformations(rawValue, detail.TransformExpr);

                        Console.WriteLine($"  列{detail.ColumnSeq} ({headers?[colIndex] ?? "N/A"}): \"{transformedValue}\"");

                        // 元値を保持
                        sourceRawDict[$"col_{detail.ColumnSeq}"] = rawValue ?? "";

                        // ステップ5: 必須チェック
                        if (detail.IsRequired && string.IsNullOrWhiteSpace(transformedValue))
                        {
                            throw new Exception($"必須項目が空です: 列{detail.ColumnSeq} ({detail.AttrCd})");
                        }

                        // if: target_column有内容 → 登录到temp_product_parsed固定字段（添加source_前缀）
                        if (!string.IsNullOrEmpty(detail.TargetColumn) && detail.TargetEntity == "PRODUCT_MST")
                        {
                            string targetFieldName = "source_" + detail.TargetColumn; // e.g., source_product_cd
                            if (SetPropertyValue(tempProduct, targetFieldName, transformedValue))
                            {
                                Console.WriteLine($"    → 固定フィールド: {targetFieldName} = {transformedValue}");
                            }
                        }

                        // else if: attr_cd有内容 → EAV可展开项目，使用 m_fixed_to_attr_map 映射，直接存入 cl_product_attr，同时备份extras
                        else if (!string.IsNullOrEmpty(detail.AttrCd) && detail.TargetEntity == "EAV" && !string.IsNullOrWhiteSpace(transformedValue))
                        {
                            var attrMaps = await _dataService.GetFixedToAttrMapsAsync(groupCompanyCd, "PRODUCT");
                            var attrMap = attrMaps.FirstOrDefault(m => m.AttrCd == detail.AttrCd);

                            var productAttr = new ClProductAttr
                            {
                                BatchId = batchId,
                                TempRowId = tempProduct.TempRowId,
                                AttrCd = detail.AttrCd,
                                AttrSeq = (short)(_productAttrs.Count(p => p.TempRowId == tempProduct.TempRowId) + 1),
                                SourceId = attrMap?.SourceIdColumn ?? "",
                                SourceLabel = attrMap?.SourceLabelColumn ?? "",
                                SourceRaw = transformedValue ?? "",
                                DataType = attrMap?.DataTypeOverride ?? "TEXT",
                                QualityFlag = "OK",
                                QualityDetailJson = "{}",
                                ProvenanceJson = JsonSerializer.Serialize(new
                                {
                                    stage = "INGEST",
                                    from = "EAV",
                                    via = attrMap != null ? "fixed_map" : "direct_map",
                                    profile_id = detail.ProfileId,
                                    column_seq = detail.ColumnSeq,
                                    map_id = attrMap?.MapId
                                }),
                                RuleVersion = "1.0"
                            };

                            _productAttrs.Add(productAttr);
                            Console.WriteLine($"    → EAV属性生成 (map): {detail.AttrCd} = {transformedValue} (source_id={attrMap?.SourceIdColumn ?? "N/A"})");
                        }

                        // else: 备份所有内容到extras_json
                        else
                        {
                            Console.WriteLine($"    → 仅备份: col_{detail.ColumnSeq} to extras_json");
                        }

                        // 所有列备份到 extras_json
                        extrasDict[$"col_{detail.ColumnSeq}"] = new
                        {
                            header = headers?[colIndex] ?? "N/A",
                            raw_value = rawValue ?? "",
                            transformed_value = transformedValue ?? "",
                            attr_cd = detail.AttrCd ?? string.Empty,
                            target_column = detail.TargetColumn ?? string.Empty,
                            target_entity = detail.TargetEntity ?? string.Empty,
                            transform_expr = detail.TransformExpr ?? string.Empty,
                            is_required = detail.IsRequired,
                            processing_stage = "INGEST"
                        };
                    }

                    // source_rawをJSONとして保存（全部数据备份）
                    tempProduct.ExtrasJson = JsonSerializer.Serialize(new
                    {
                        source_raw = sourceRawDict,
                        processed_columns = extrasDict,
                        headers = headers,
                        processing_timestamp = DateTime.UtcNow
                    });

                    // ステップ6: tempへの保存
                    _tempProducts.Add(tempProduct);
                    okCount++;
                    Console.WriteLine($"  ✅ 取込成功 (TempRowId: {tempProduct.TempRowId})");

                }
                catch (Exception ex)
                {
                    ngCount++;
                    var error = new RecordError
                    {
                        BatchId = batchId,
                        Step = "INGEST",
                        RecordRef = $"line:{dataRowNumber}",
                        ErrorCd = "PARSE_FAILED",
                        ErrorDetail = ex.Message,
                        RawFragment = csv.Context.Parser.RawRecord ?? ""
                    };
                    _recordErrors.Add(error);
                    Console.WriteLine($"  ❌ エラー: {ex.Message}");
                }
            }

            // データベースに保存
            await SaveTempProductsToDatabase(_tempProducts);
            await SaveRecordErrorsToDatabase(_recordErrors);

            Console.WriteLine($"\n✓ データ処理完了: 読込={readCount}, 成功={okCount}, エラー={ngCount}");

            return (readCount, okCount, ngCount);
        }

        // 解析跳过行字符串为HashSet（逗号分隔特定行号，只跳过指定行）。
        private HashSet<long> ParseSkipRows(string skipRows)
        {
            var skipSet = new HashSet<long>();
            if (!string.IsNullOrEmpty(skipRows))
            {
                var skipRowStrings = skipRows.Split(',');
                foreach (var rowStr in skipRowStrings)
                {
                    if (long.TryParse(rowStr.Trim(), out long skipRow))
                    {
                        skipSet.Add(skipRow);
                    }
                }
            }
            return skipSet;
        }

        // ステップ7-9: 属性マッピングとcl_product_attr作成（第三步后续：EAV生成及固定字段转换）
        private async Task Step7To9_CreateProductAttributes(string batchId, string groupCompanyCd, List<MDataImportD> importDetails)
        {
            Console.WriteLine("\n--- ステップ7-9: 属性マッピングとcl_product_attr作成 ---");

            // 由于第三步已处理 EAV，这里只 fallback 固定字段未映射的
            var attrMaps = await _dataService.GetFixedToAttrMapsAsync(groupCompanyCd, "PRODUCT");
            Console.WriteLine($"✓ 属性マップ数: {attrMaps.Count}");

            short attrSeq = 0;

            foreach (var tempProduct in _tempProducts)
            {
                // 解析extras_json
                var extrasRoot = JsonSerializer.Deserialize<Dictionary<string, object>>(tempProduct.ExtrasJson ?? "{}");
                var processedColumns = extrasRoot?["processed_columns"] != null 
                    ? JsonSerializer.Deserialize<Dictionary<string, object>>(extrasRoot["processed_columns"].ToString() ?? "{}")
                    : new Dictionary<string, object>();

                // A: 固定字段 fallback (如果第三步未生成)
                foreach (var detail in importDetails.Where(d => !string.IsNullOrEmpty(d.TargetColumn)))
                {
                    var attrMap = attrMaps.FirstOrDefault(m => m.AttrCd == detail.AttrCd);
                    if (attrMap != null)
                    {
                        attrSeq++;

                        // 从固定字段获取值
                        string targetFieldName = "source_" + detail.TargetColumn;
                        var value = GetPropertyValue(tempProduct, targetFieldName);

                        // fallback 从 extras
                        if (string.IsNullOrEmpty(value) && processedColumns.ContainsKey($"col_{detail.ColumnSeq}"))
                        {
                            var colData = JsonSerializer.Deserialize<Dictionary<string, object>>(
                                processedColumns[$"col_{detail.ColumnSeq}"].ToString() ?? "{}");
                            value = colData?["transformed_value"]?.ToString() ?? "";
                        }

                        if (!string.IsNullOrEmpty(value))
                        {
                            var productAttr = new ClProductAttr
                            {
                                BatchId = batchId,
                                TempRowId = tempProduct.TempRowId,
                                AttrCd = detail.AttrCd,
                                AttrSeq = attrSeq,
                                SourceId = attrMap.SourceIdColumn,
                                SourceLabel = attrMap.SourceLabelColumn,
                                SourceRaw = value,
                                DataType = attrMap.DataTypeOverride ?? "TEXT",
                                QualityFlag = "OK",
                                QualityDetailJson = "{}",
                                ProvenanceJson = JsonSerializer.Serialize(new
                                {
                                    stage = "INGEST",
                                    from = "FIXED_FIELD",
                                    via = "fixed_map",
                                    profile_id = detail.ProfileId,
                                    map_id = attrMap.MapId,
                                    target_column = detail.TargetColumn
                                }),
                                RuleVersion = "1.0"
                            };

                            _productAttrs.Add(productAttr);
                            Console.WriteLine($"  ✅ 固定属性投影 (fallback): {detail.AttrCd} = {value} (from {targetFieldName})");
                        }
                    }
                }

                // B & C: EAV 和备份已在第三步处理，这里跳过
                Console.WriteLine($"  📝 EAV/备份已在第三步处理 (extras_json 已备份)");
            }

            // ステップ9: データベースに保存
            await SaveProductAttrsToDatabase(_productAttrs);
            Console.WriteLine($"✓ cl_product_attr保存完了: {_productAttrs.Count} レコード");
        }

        // ステップ10: バッチ統計更新。
        private async Task Step10_UpdateBatchStatistics(string batchId, (int readCount, int okCount, int ngCount) result)
        {
            Console.WriteLine("\n--- ステップ10: バッチ統計更新 ---");

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

                // データベース更新
                await UpdateBatchRunInDatabase(batchRun);

                Console.WriteLine($"✓ バッチ統計更新: 状態={batchRun.BatchStatus}");
                Console.WriteLine($"  読込: {result.readCount}, 成功: {result.okCount}, エラー: {result.ngCount}");
            }
        }

        // 标记批处理为失败。
        private async Task MarkBatchAsFailed(string batchId, string errorMessage)
        {
            var batchRun = _batchRuns.FirstOrDefault(b => b.BatchId == batchId);
            if (batchRun != null)
            {
                batchRun.BatchStatus = "FAILED";
                batchRun.EndedAt = DateTime.UtcNow;
                await UpdateBatchRunInDatabase(batchRun);
                Console.WriteLine($"❌ バッチ失敗: {errorMessage}");
            }
        }

        // 应用转换表达式到值。
        private string? ApplyTransformations(string? value, string transformExpr)
        {
            if (string.IsNullOrEmpty(value)) return value;

            var result = value.Trim().Trim('\u3000'); // 全角スペースもトリム

            if (!string.IsNullOrEmpty(transformExpr))
            {
                if (transformExpr.Contains("trim(@)"))
                {
                    result = result.Trim();
                }
                if (transformExpr.Contains("upper(@)"))
                {
                    result = result.ToUpper();
                }
                if (transformExpr.Contains("lower(@)"))
                {
                    result = result.ToLower();
                }
            }

            return result;
        }

        // 设置对象属性值。
        private bool SetPropertyValue(TempProductParsed obj, string propertyName, string? value)
        {
            try
            {
                var property = typeof(TempProductParsed).GetProperty(
                    propertyName,
                    BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance
                );
                
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

        // 获取对象属性值。
        private string? GetPropertyValue(TempProductParsed obj, string propertyName)
        {
            try
            {
                var property = typeof(TempProductParsed).GetProperty(
                    propertyName,
                    BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance
                );
                
                return property?.GetValue(obj) as string;
            }
            catch
            {
                return null;
            }
        }

        // 根据字符代码获取编码。
        private Encoding GetEncoding(string characterCd)
        {
            return characterCd.ToUpperInvariant() switch
            {
                "UTF-8" => Encoding.UTF8,
                "SHIFT_JIS" => Encoding.GetEncoding("Shift_JIS"),
                "EUC-JP" => Encoding.GetEncoding("EUC-JP"),
                _ => Encoding.UTF8
            };
        }

        // 保存批处理运行到数据库。
        private async Task SaveBatchRunToDatabase(BatchRun batchRun)
        {
            try
            {
                using var connection = new NpgsqlConnection(_connectionString);
                await connection.OpenAsync();
                
                var parameters = new
                {
                    BatchId = batchRun.BatchId,
                    IdemKey = batchRun.IdemKey,
                    S3Bucket = batchRun.S3Bucket,
                    Etag = batchRun.Etag,
                    GroupCompanyCd = batchRun.GroupCompanyCd,
                    DataKind = batchRun.DataKind,
                    FileKey = batchRun.FileKey,
                    BatchStatus = batchRun.BatchStatus,
                    CountsJson = batchRun.CountsJson,
                    StartedAt = batchRun.StartedAt,
                    EndedAt = batchRun.EndedAt,
                    CreAt = DateTime.UtcNow,
                    UpdAt = DateTime.UtcNow
                };
                
                var sql = @"INSERT INTO batch_run 
                            (batch_id, idem_key, s3_bucket, etag, group_company_cd, 
                             data_kind, file_key, batch_status, counts_json, 
                             started_at, ended_at, cre_at, upd_at) 
                            VALUES (@BatchId, @IdemKey, @S3Bucket, @Etag, @GroupCompanyCd, 
                                    @DataKind, @FileKey, @BatchStatus, @CountsJson::jsonb, 
                                    @StartedAt, @EndedAt, @CreAt, @UpdAt)";
                
                await connection.ExecuteAsync(sql, parameters);
                
                Console.WriteLine($"✅ バッチ情報をデータベースに保存しました: {batchRun.BatchId}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ バッチ情報保存エラー: {ex.Message}");
                throw;
            }
        }

        // 更新批处理运行在数据库。
        private async Task UpdateBatchRunInDatabase(BatchRun batchRun)
        {
            try
            {
                using var connection = new NpgsqlConnection(_connectionString);
                await connection.OpenAsync();
                
                var parameters = new
                {
                    BatchId = batchRun.BatchId,
                    BatchStatus = batchRun.BatchStatus,
                    CountsJson = batchRun.CountsJson,
                    EndedAt = batchRun.EndedAt,
                    UpdAt = DateTime.UtcNow
                };
                
                var sql = @"UPDATE batch_run 
                            SET batch_status = @BatchStatus, 
                                counts_json = @CountsJson::jsonb,
                                ended_at = @EndedAt,
                                upd_at = @UpdAt
                            WHERE batch_id = @BatchId";
                
                await connection.ExecuteAsync(sql, parameters);
                
                Console.WriteLine($"✅ バッチ状態を更新しました: {batchRun.BatchId} -> {batchRun.BatchStatus}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ バッチ状態更新エラー: {ex.Message}");
                throw;
            }
        }

        // 保存临时产品到数据库（简易实现）。
        private async Task SaveTempProductsToDatabase(List<TempProductParsed> products)
        {
            Console.WriteLine($"✓ temp_product_parsed保存: {products.Count} レコード");
            await Task.CompletedTask;
        }

        // 保存产品属性到数据库（简易实现）。
        private async Task SaveProductAttrsToDatabase(List<ClProductAttr> attrs)
        {
            Console.WriteLine($"✓ cl_product_attr保存: {attrs.Count} レコード");
            await Task.CompletedTask;
        }

        // 保存记录错误到数据库（简易实现）。
        private async Task SaveRecordErrorsToDatabase(List<RecordError> errors)
        {
            Console.WriteLine($"✓ record_error保存: {errors.Count} レコード");
            await Task.CompletedTask;
        }
    }
}