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

                // 🔍 添加调试信息：显示TempProductParsed的属性结构
                Console.WriteLine($"\n🔍 调试信息:");
                Console.WriteLine($"TempProductParsed属性列表:");
                var allSourceProperties = typeof(TempProductParsed).GetProperties()
                    .Where(p => p.Name.StartsWith("Source") && p.CanWrite)
                    .OrderBy(p => p.Name)
                    .Select(p => p.Name)
                    .ToList();
                
                int count = 0;
                foreach (var prop in allSourceProperties)
                {
                    Console.WriteLine($"  - {prop}");
                    count++;
                    if (count >= 15) // 只显示前15个属性
                    {
                        Console.WriteLine($"  ... 还有 {allSourceProperties.Count - 15} 个属性");
                        break;
                    }
                }

                // 🔍 添加调试信息：显示映射配置
                Console.WriteLine($"\n📋 映射配置详情:");
                Console.WriteLine($"importDetails 数量: {importDetails.Count}");
                var productMstMappings = importDetails.Where(d => d.TargetEntity == "PRODUCT_MST").ToList();
                Console.WriteLine($"PRODUCT_MST 映射数量: {productMstMappings.Count}");
                
                foreach (var mapping in productMstMappings.Take(10)) // 只显示前10个
                {
                    string expectedFieldName = "Source" + ToPascalCase(mapping.TargetColumn ?? "");
                    Console.WriteLine($"  列{mapping.ColumnSeq} -> TargetColumn='{mapping.TargetColumn}' -> {expectedFieldName} (Attr: {mapping.AttrCd})");
                }
                if (productMstMappings.Count > 10)
                {
                    Console.WriteLine($"  ... 还有 {productMstMappings.Count - 10} 个映射");
                }

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

        // 添加辅助方法：snake_case转PascalCase
        private string ToPascalCase(string input)
        {
            if (string.IsNullOrEmpty(input))
                return input;

            var parts = input.Split(new char[] { '_', '-' }, StringSplitOptions.RemoveEmptyEntries);
            return string.Concat(parts.Select(part => char.ToUpperInvariant(part[0]) + part.Substring(1).ToLowerInvariant()));
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

        private async Task<(int readCount, int okCount, int ngCount)> Step4To6_ProcessCsvAndSaveToTemp(
            string filePath, string batchId, string groupCompanyCd,
            MDataImportSetting importSetting, List<MDataImportD> importDetails, CsvConfiguration config)
        {
            Console.WriteLine("\n--- ステップ4-6: CSV読込・変換・必須チェック・temp保存 ---");

            int readCount = 0, okCount = 0, ngCount = 0;

            using var reader = new StreamReader(filePath, config.Encoding ?? Encoding.UTF8);
            using var csv = new CsvReader(reader, config);

            // ヘッダー処理
            string[]? headers = null;
            long currentLine = 0;

            // ヘッダー行まで読み進める
            while (currentLine < importSetting.HeaderRowIndex)
            {
                if (await csv.ReadAsync())
                {
                    currentLine++;
                    if (currentLine == importSetting.HeaderRowIndex)
                    {
                        headers = csv.Parser.Record;
                        Console.WriteLine($"✓ ヘッダー行読み込み (行 {currentLine}): {headers?.Length} 列");
                        
                        // 调试：显示列映射
                        Console.WriteLine($"\n📋 列映射配置:");
                        foreach (var detail in importDetails.OrderBy(d => d.ColumnSeq))
                        {
                            string headerName = (headers != null && detail.ColumnSeq < headers.Length) 
                                ? headers[detail.ColumnSeq] ?? "N/A" 
                                : "N/A";
                            Console.WriteLine($"  列{detail.ColumnSeq}: '{headerName}' -> {detail.TargetEntity}.{detail.TargetColumn} (Attr: {detail.AttrCd})");
                        }
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

            // スキップ行の処理
            var skipRows = ParseSkipRows(importSetting.SkipRows);
            Console.WriteLine($"スキップ対象行: {(skipRows.Any() ? string.Join(", ", skipRows) : "なし")}");

            Console.WriteLine($"\n--- データ行処理開始 ---");

            // データ行処理
            long dataRowNumber = 0;

            while (await csv.ReadAsync())
            {
                currentLine++;
                dataRowNumber++;

                // 跳过指定行
                if (skipRows.Contains(dataRowNumber))
                {
                    Console.WriteLine($"⏩ データ行 {dataRowNumber} をスキップ (設定によるスキップ)");
                    continue;
                }

                readCount++;

                try
                {
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
                    var requiredFields = new Dictionary<string, string>();

                    // 按照 m_data_import_d 规则处理每一列
                    foreach (var detail in importDetails.OrderBy(d => d.ColumnSeq))
                    {
                        // ColumnSeq 在数据库中可能是 0-based 或 1-based，不确定时尝试两种索引
                        int colIndex = detail.ColumnSeq;
                        string? rawValue = null;
                        var record = csv.Parser.Record;

                        if (record != null)
                        {
                            if (colIndex >= 0 && colIndex < record.Length)
                            {
                                rawValue = record[colIndex];
                            }
                            else if (colIndex - 1 >= 0 && colIndex - 1 < record.Length)
                            {
                                // 支持 ColumnSeq 存 1-based 的情况
                                rawValue = record[colIndex - 1];
                                Console.WriteLine($"    注意: ColumnSeq {detail.ColumnSeq} 看起来像 1-based，已使用 index {colIndex - 1}");
                            }
                        }

                        if (rawValue == null)
                        {
                            Console.WriteLine($"  列{detail.ColumnSeq}: [範囲外または空]");
                            continue;
                        }

                        // 步骤4: 应用转换表达式（兼容 null）
                        string? transformedValue = ApplyTransformations(rawValue, detail.TransformExpr ?? "");

                        Console.WriteLine($"  列{detail.ColumnSeq} ({headers?[colIndex] ?? "N/A"}): \"{rawValue}\" -> \"{transformedValue}\"");

                        // 保存原始值
                        sourceRawDict[$"col_{detail.ColumnSeq}"] = rawValue ?? "";

                        // 步骤5: 必须字段检查
                        if (detail.IsRequired && string.IsNullOrWhiteSpace(transformedValue))
                        {
                            requiredFields[detail.AttrCd ?? detail.TargetColumn ?? $"col_{detail.ColumnSeq}"] = 
                                $"必須項目が空です: {detail.AttrCd ?? detail.TargetColumn}";
                        }

                        bool? mappingSuccess = null;

                        // 步骤6: 根据 target_entity 处理数据 - 修复这里的逻辑
                        // 兼容多种配置：有的 profile 会使用 "PRODUCT_MST"，有的使用 "PRODUCT"
                        if (!string.IsNullOrEmpty(detail.TargetColumn) &&
                            (string.Equals(detail.TargetEntity, "PRODUCT_MST", StringComparison.OrdinalIgnoreCase)
                             || string.Equals(detail.TargetEntity, "PRODUCT", StringComparison.OrdinalIgnoreCase)))
                        {
                            // 统一使用大写S开头，并转换为PascalCase
                            string targetFieldName = "Source" + ToPascalCase(detail.TargetColumn);
                            Console.WriteLine($"    尝试映射: TargetColumn='{detail.TargetColumn}' -> {targetFieldName}");
                            
                            mappingSuccess = SetPropertyValue(tempProduct, targetFieldName, transformedValue);
                            
                            if (mappingSuccess.Value)
                            {
                                Console.WriteLine($"    → 固定フィールド映射成功: {targetFieldName} = '{transformedValue ?? "(空)"}'");
                            }
                            else
                            {
                                Console.WriteLine($"    ❌ 固定フィールド映射失败: {targetFieldName}");
                                // 调试：显示可用的属性
                                var properties = typeof(TempProductParsed).GetProperties()
                                    .Where(p => p.Name.StartsWith("Source"))
                                    .Select(p => p.Name)
                                    .ToList();
                                Console.WriteLine($"      可用字段 (前10): {string.Join(", ", properties.Take(10))}");
                                if (properties.Count > 10)
                                    Console.WriteLine($"      ... 总计 {properties.Count} 个字段");
                            }
                        }
                        else if (detail.TargetEntity == "EAV" && !string.IsNullOrEmpty(detail.AttrCd))
                        {
                            // EAV字段处理...
                            var productAttr = new ClProductAttr
                            {
                                BatchId = batchId,
                                TempRowId = tempProduct.TempRowId,
                                AttrCd = detail.AttrCd,
                                AttrSeq = (short)(_productAttrs.Count(p => p.TempRowId == tempProduct.TempRowId && p.AttrCd == detail.AttrCd) + 1),
                                SourceId = $"col_{detail.ColumnSeq}",
                                SourceLabel = headers?[colIndex] ?? $"Column_{detail.ColumnSeq}",
                                SourceRaw = transformedValue ?? "",
                                ValueText = transformedValue ?? "", // 修复：设置ValueText
                                DataType = "TEXT",
                                QualityFlag = string.IsNullOrWhiteSpace(transformedValue) ? "REVIEW" : "OK",
                                QualityDetailJson = JsonSerializer.Serialize(new
                                {
                                    empty_value = string.IsNullOrWhiteSpace(transformedValue),
                                    processing_stage = "INGEST",
                                    is_required = detail.IsRequired
                                }),
                                ProvenanceJson = JsonSerializer.Serialize(new
                                {
                                    stage = "INGEST",
                                    from = "EAV",
                                    via = "direct_map",
                                    profile_id = detail.ProfileId,
                                    column_seq = detail.ColumnSeq,
                                    transform_expr = detail.TransformExpr
                                }),
                                RuleVersion = "1.0"
                            };

                            _productAttrs.Add(productAttr);
                            Console.WriteLine($"    → EAV属性生成: {detail.AttrCd} = '{transformedValue ?? "(空)"}'");
                        }

                        // 所有处理信息保存到 extras_json
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
                            processing_stage = "INGEST",
                            processing_result = GetProcessingResult(detail, transformedValue),
                            mapping_success = mappingSuccess
                        };
                    }

                    // 步骤5: 必须字段检查失败处理
                    if (requiredFields.Any())
                    {
                        throw new Exception($"必須項目エラー: {string.Join("; ", requiredFields.Values)}");
                    }

                    // 调试：显示tempProduct的实际数据 (更多属性)
                    Console.WriteLine($"  🔍 TempProduct数据验证 (部分属性):");
                    var sampleProps = new[] { "SourceProductCd", "SourceBrandNm", "SourceCategory1Nm", "SourceQuantity", "SourcePurchasePriceExclTax" };
                    foreach (var propName in sampleProps)
                    {
                        var prop = typeof(TempProductParsed).GetProperty(propName);
                        var value = prop?.GetValue(tempProduct) as string ?? "null";
                        Console.WriteLine($"    - {propName}: '{value}'");
                    }

                    // 保存原始数据到 extras_json
                    tempProduct.ExtrasJson = JsonSerializer.Serialize(new
                    {
                        source_raw = sourceRawDict,
                        processed_columns = extrasDict,
                        headers = headers,
                        processing_timestamp = DateTime.UtcNow,
                        required_fields_check = requiredFields
                    }, new JsonSerializerOptions { WriteIndented = false });

                    // 步骤6: 保存到临时表
                    _tempProducts.Add(tempProduct);
                    okCount++;
                    Console.WriteLine($"  ✅ 取込成功 (TempRowId: {tempProduct.TempRowId})");

                }
                catch (Exception ex)
                {
                    ngCount++;
                    var rawFragment = string.Empty;
                    try { rawFragment = csv.Context?.Parser?.RawRecord ?? string.Empty; } catch { rawFragment = string.Empty; }
                    var error = new RecordError
                    {
                        BatchId = batchId,
                        Step = "INGEST",
                        RecordRef = $"line:{dataRowNumber}",
                        ErrorCd = ex.Message.Contains("必須項目") ? "MISSING_REQUIRED_FIELD" : "PARSE_FAILED",
                        ErrorDetail = ex.Message,
                        RawFragment = rawFragment
                    };
                    _recordErrors.Add(error);
                    Console.WriteLine($"  ❌ エラー: {ex.Message}");
                }
            }

            // 保存到数据库
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

            foreach (var tempProduct in _tempProducts)
            {
                // 解析extras_json
                var extrasRoot = JsonSerializer.Deserialize<Dictionary<string, object>>(tempProduct.ExtrasJson ?? "{}") ?? new Dictionary<string, object>();
                var processedColumns = new Dictionary<string, object>();
                if (extrasRoot.ContainsKey("processed_columns") && extrasRoot["processed_columns"] != null)
                {
                    try
                    {
                        processedColumns = JsonSerializer.Deserialize<Dictionary<string, object>>(extrasRoot["processed_columns"].ToString() ?? "{}") ?? new Dictionary<string, object>();
                    }
                    catch
                    {
                        processedColumns = new Dictionary<string, object>();
                    }
                }

                // A: 固定字段 fallback (如果第三步未生成)
                foreach (var detail in importDetails.Where(d => !string.IsNullOrEmpty(d.TargetColumn)))
                {
                    var attrMap = attrMaps.FirstOrDefault(m => m.AttrCd == detail.AttrCd);
                        if (attrMap != null)
                        {
                            // 计算该 temp_row_id 下同一 attr_cd 的序号 (attr_seq 从 1 开始)
                            short attrSeqForRow = (short)(_productAttrs.Count(p => p.TempRowId == tempProduct.TempRowId && p.AttrCd == detail.AttrCd) + 1);

                            // 从固定字段获取值 - 统一使用大写S，并转换为PascalCase
                            string targetFieldName = "Source" + ToPascalCase(detail.TargetColumn);
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
                                AttrSeq = attrSeqForRow,
                                SourceId = attrMap.SourceIdColumn,
                                SourceLabel = attrMap.SourceLabelColumn,
                                SourceRaw = value,
                                ValueText = value, // 修复：设置ValueText
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
                // 支持多种转换表达式
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
                // 可以添加更多转换规则
                if (transformExpr.Contains("to_timestamp(@,'YYYY-MM-DD')"))
                {
                    // 日期转换逻辑
                }
            }

            return result;
        }

        // 设置对象属性值 - 改进版本，添加更多调试
        private bool SetPropertyValue(TempProductParsed obj, string propertyName, string? value)
        {
            try
            {
                // 首先尝试精确匹配
                var property = typeof(TempProductParsed).GetProperty(propertyName);
                if (property != null && property.CanWrite)
                {
                    property.SetValue(obj, value);
                    Console.WriteLine($"    ✅ 精确匹配成功: {propertyName} = '{value}'");
                    return true;
                }
                
                // 尝试忽略大小写匹配
                property = typeof(TempProductParsed).GetProperty(propertyName, 
                    BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance);
                
                if (property != null && property.CanWrite)
                {
                    property.SetValue(obj, value);
                    Console.WriteLine($"    ✅ 忽略大小写匹配成功: {propertyName} = '{value}'");
                    return true;
                }
                
                Console.WriteLine($"⚠️ 属性不存在: {propertyName}");
                
                // 额外调试：列出所有可写Source属性
                var allProps = typeof(TempProductParsed).GetProperties()
                    .Where(p => p.Name.StartsWith("Source") && p.CanWrite)
                    .Select(p => p.Name)
                    .OrderBy(n => n)
                    .ToList();
                Console.WriteLine($"    所有可用Source属性 ({allProps.Count}): {string.Join(", ", allProps)}");
                
                return false;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ 设置属性失败 {propertyName}: {ex.Message}");
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

        // 实际的保存方法 - 替换现有的模拟方法
        private async Task SaveTempProductsToDatabase(List<TempProductParsed> products)
        {
            if (products.Count == 0) return;

            try
            {
                using var connection = new NpgsqlConnection(_connectionString);
                await connection.OpenAsync();

                var insertSql = @"
                    INSERT INTO temp_product_parsed (
                        temp_row_id, batch_id, line_no, source_group_company_cd,
                        source_product_cd, source_product_management_cd,
                        source_brand_id, source_brand_nm,
                        source_category_1_id, source_category_1_nm,
                        source_category_2_id, source_category_2_nm,
                        source_category_3_id, source_category_3_nm,
                        source_product_status_cd, source_product_status_nm,
                        source_new_used_kbn, source_quantity,
                        source_stock_existence_cd, source_stock_existence_nm,
                        source_sale_permission_cd, source_sale_permission_nm,
                        source_transfer_status, source_repair_status,
                        source_reservation_status, source_consignment_status,
                        source_accept_status, source_ec_listing_kbn,
                        source_assessment_price_excl_tax, source_assessment_price_incl_tax,
                        source_assessment_tax_rate, source_purchase_price_excl_tax,
                        source_purchase_price_incl_tax, source_purchase_tax_rate,
                        source_display_price_excl_tax, source_display_price_incl_tax,
                        source_display_tax_rate, source_sales_price_excl_tax,
                        source_sales_price_incl_tax, source_sales_tax_rate,
                        source_purchase_rank, source_purchase_rank_name,
                        source_sales_rank, source_sales_rank_name,
                        source_sales_channel_nm, source_sales_channel_region,
                        source_sales_channel_method, source_sales_channel_target,
                        source_purchase_channel_nm, source_purchase_channel_region,
                        source_purchase_channel_method, source_purchase_channel_target,
                        source_store_id, source_store_nm,
                        source_consignor_group_company_id, source_consignor_product_cd,
                        extras_json, step_status
                    ) VALUES (
                        @TempRowId, @BatchId, @LineNo, @SourceGroupCompanyCd,
                        @SourceProductCd, @SourceProductManagementCd,
                        @SourceBrandId, @SourceBrandNm,
                        @SourceCategory1Id, @SourceCategory1Nm,
                        @SourceCategory2Id, @SourceCategory2Nm,
                        @SourceCategory3Id, @SourceCategory3Nm,
                        @SourceProductStatusCd, @SourceProductStatusNm,
                        @SourceNewUsedKbn, @SourceQuantity,
                        @SourceStockExistenceCd, @SourceStockExistenceNm,
                        @SourceSalePermissionCd, @SourceSalePermissionNm,
                        @SourceTransferStatus, @SourceRepairStatus,
                        @SourceReservationStatus, @SourceConsignmentStatus,
                        @SourceAcceptStatus, @SourceEcListingKbn,
                        @SourceAssessmentPriceExclTax, @SourceAssessmentPriceInclTax,
                        @SourceAssessmentTaxRate, @SourcePurchasePriceExclTax,
                        @SourcePurchasePriceInclTax, @SourcePurchaseTaxRate,
                        @SourceDisplayPriceExclTax, @SourceDisplayPriceInclTax,
                        @SourceDisplayTaxRate, @SourceSalesPriceExclTax,
                        @SourceSalesPriceInclTax, @SourceSalesTaxRate,
                        @SourcePurchaseRank, @SourcePurchaseRankName,
                        @SourceSalesRank, @SourceSalesRankName,
                        @SourceSalesChannelNm, @SourceSalesChannelRegion,
                        @SourceSalesChannelMethod, @SourceSalesChannelTarget,
                        @SourcePurchaseChannelNm, @SourcePurchaseChannelRegion,
                        @SourcePurchaseChannelMethod, @SourcePurchaseChannelTarget,
                        @SourceStoreId, @SourceStoreNm,
                        @SourceConsignorGroupCompanyId, @SourceConsignorProductCd,
                        @ExtrasJson::jsonb, @StepStatus
                    ) ON CONFLICT (temp_row_id) DO NOTHING";

                await connection.ExecuteAsync(insertSql, products);
                Console.WriteLine($"成功保存 {products.Count} 条商品数据到临时表");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"保存到临时表时发生错误: {ex.Message}");
                Console.WriteLine($"详细错误: {ex}");
                throw;
            }
        }

        // 同様に SaveProductAttrsToDatabase メソッドも修正
        private async Task SaveProductAttrsToDatabase(List<ClProductAttr> attrs)
        {
            if (attrs.Count == 0) return;

            try
            {
                using var connection = new NpgsqlConnection(_connectionString);
                await connection.OpenAsync();

                var sql = @"
                    INSERT INTO cl_product_attr (
                        batch_id, temp_row_id, attr_cd, attr_seq,
                        source_id, source_label, source_raw, value_text,
                        value_num, value_date, value_cd, g_list_item_id,
                        data_type, quality_flag, quality_detail_json, provenance_json,
                        rule_version, cre_at, upd_at
                    ) VALUES (
                        @BatchId, @TempRowId, @AttrCd, @AttrSeq,
                        @SourceId, @SourceLabel, @SourceRaw, @ValueText,
                        @ValueNum, @ValueDate, @ValueCd, @GListItemId,
                        @DataType, @QualityFlag, @QualityDetailJson::jsonb, @ProvenanceJson::jsonb,
                        @RuleVersion, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    ) ON CONFLICT (batch_id, temp_row_id, attr_cd, attr_seq) DO NOTHING";

                await connection.ExecuteAsync(sql, attrs);
                Console.WriteLine($"✅ cl_product_attr保存: {attrs.Count} レコード");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ cl_product_attr保存エラー: {ex.Message}");
                throw;
            }
        }

        // 保存记录错误到数据库（简易实现）。
        private async Task SaveRecordErrorsToDatabase(List<RecordError> errors)
        {
            if (errors.Count == 0) return;

            try
            {
                using var connection = new NpgsqlConnection(_connectionString);
                await connection.OpenAsync();

                var sql = @"
                    INSERT INTO record_error (
                        batch_id, step, record_ref, error_cd, error_detail, raw_fragment,
                        cre_at, upd_at
                    ) VALUES (
                        @BatchId, @Step, @RecordRef, @ErrorCd, @ErrorDetail, @RawFragment,
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    ) ON CONFLICT (batch_id) DO NOTHING";

                await connection.ExecuteAsync(sql, errors);
                Console.WriteLine($"✅ record_error保存: {errors.Count} レコード");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ record_error保存エラー: {ex.Message}");
                throw;
            }
        }

        // 获取处理结果描述
        private string GetProcessingResult(MDataImportD detail, string? value)
        {
            if (!string.IsNullOrEmpty(detail.TargetColumn))
                return $"FIXED_FIELD:Source{ToPascalCase(detail.TargetColumn)}";
            else if (!string.IsNullOrEmpty(detail.AttrCd))
                return $"EAV_ATTR:{detail.AttrCd}";
            else
                return "BACKUP_ONLY";
        }
    }
}