using System.Text.Json;
using ProductDataIngestion.Models;

namespace ProductDataIngestion.Services
{
    /// <summary>
    /// 属性処理クラス
    /// extras_jsonからデータを取得してcl_product_attrに挿入する処理を担当
    /// </summary>
    public class AttributeProcessor
    {
        private readonly DataImportService _dataService;

        public AttributeProcessor(DataImportService dataService)
        {
            _dataService = dataService;
        }

        /// <summary>
        /// extras_jsonから処理済み列情報を抽出
        /// </summary>
        public Dictionary<string, ProcessedColumnInfo> ExtractProcessedColumns(string extrasJson)
        {
            try
            {
                var extrasRoot = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(extrasJson);
                if (extrasRoot == null || !extrasRoot.ContainsKey("processed_columns"))
                {
                    return new Dictionary<string, ProcessedColumnInfo>();
                }

                var processedColumns = new Dictionary<string, ProcessedColumnInfo>();
                var processedColumnsElement = extrasRoot["processed_columns"];

                foreach (var property in processedColumnsElement.EnumerateObject())
                {
                    var columnInfo = JsonSerializer.Deserialize<ProcessedColumnInfo>(property.Value.GetRawText());
                    if (columnInfo != null)
                    {
                        processedColumns[property.Name] = columnInfo;
                    }
                }

                return processedColumns;
            }
            catch (Exception ex)
            {
                throw new IngestException(
                    ErrorCodes.PARSE_FAILED,
                    $"extras_jsonの解析に失敗しました: {ex.Message}",
                    ex
                );
            }
        }

        /// <summary>
        /// extras_jsonからsource_rawを抽出
        /// </summary>
        public Dictionary<string, string> ExtractSourceRaw(string extrasJson)
        {
            try
            {
                var extrasRoot = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(extrasJson);
                if (extrasRoot == null || !extrasRoot.ContainsKey("source_raw"))
                {
                    return new Dictionary<string, string>();
                }

                var sourceRaw = JsonSerializer.Deserialize<Dictionary<string, string>>(
                    extrasRoot["source_raw"].GetRawText()
                );

                return sourceRaw ?? new Dictionary<string, string>();
            }
            catch (Exception ex)
            {
                throw new IngestException(
                    ErrorCodes.PARSE_FAILED,
                    $"source_rawの解析に失敗しました: {ex.Message}",
                    ex
                );
            }
        }

        /// <summary>
        /// 属性処理: PRODUCT と PRODUCT_EAV の両方に対応
        /// extras_jsonから直接データを取得して処理
        /// </summary>
        public async Task<List<ClProductAttr>> ProcessAttributesAsync(
            string batchId,
            TempProductParsed tempProduct,
            string groupCompanyCd,
            string dataKind)
        {
            var productAttrs = new List<ClProductAttr>();

            try
            {
                // extras_jsonから処理済み列情報を抽出
                Console.WriteLine($"\n=== extras_json解析開始 (temp_row_id={tempProduct.TempRowId}) ===");
                var processedColumns = ExtractProcessedColumns(tempProduct.ExtrasJson);
                var sourceRaw = ExtractSourceRaw(tempProduct.ExtrasJson);

                Console.WriteLine($"✓ extras_jsonから抽出: processed_columns={processedColumns.Count}件, source_raw={sourceRaw.Count}件");

                // processed_columnsの内容を出力
                foreach (var kvp in processedColumns.Take(5))
                {
                    Console.WriteLine($"  [{kvp.Key}] header={kvp.Value.Header}, attr_cd={kvp.Value.AttrCd}, " +
                                    $"projection_kind={kvp.Value.ProjectionKind}, is_required={kvp.Value.IsRequired}, " +
                                    $"transformed_value={kvp.Value.TransformedValue}");
                }
                if (processedColumns.Count > 5)
                {
                    Console.WriteLine($"  ... 他 {processedColumns.Count - 5} 件");
                }

                // マスタデータの取得
                var attrMaps = await _dataService.GetFixedToAttrMapsAsync(groupCompanyCd, dataKind);
                var attrDefinitions = await _dataService.GetAttrDefinitionsAsync();

                // is_required == trueの列のみを処理
                // PRODUCTとPRODUCT_EAVの両方を処理
                var requiredColumns = processedColumns
                    .Where(kvp => kvp.Value.IsRequired &&
                                 (kvp.Value.ProjectionKind == "PRODUCT" || kvp.Value.ProjectionKind == "PRODUCT_EAV"))
                    .OrderBy(kvp => kvp.Value.CsvColumnIndex)
                    .ToList();

                Console.WriteLine($"\n✓ フィルタ後の処理対象列数: {requiredColumns.Count} (PRODUCT + PRODUCT_EAV)");

                foreach (var columnKvp in requiredColumns)
                {
                    var columnInfo = columnKvp.Value;

                    Console.WriteLine($"\n--- 処理中: {columnKvp.Key} ---");
                    Console.WriteLine($"  attr_cd={columnInfo.AttrCd}, header={columnInfo.Header}");
                    Console.WriteLine($"  transformed_value='{columnInfo.TransformedValue}'");
                    Console.WriteLine($"  projection_kind={columnInfo.ProjectionKind}, is_required={columnInfo.IsRequired}");

                    // attr_cdが空の場合はエラー (is_required=trueの列には必須)
                    if (string.IsNullOrEmpty(columnInfo.AttrCd))
                    {
                        throw new IngestException(
                            ErrorCodes.MAPPING_NOT_FOUND,
                            $"必須列 {columnInfo.CsvColumnIndex} ({columnInfo.Header}) の attr_cd が m_data_import_d で定義されていません",
                            recordRef: $"temp_row_id:{tempProduct.TempRowId}"
                        );
                    }

                    // 変換後の値が空の場合はスキップ
                    if (string.IsNullOrWhiteSpace(columnInfo.TransformedValue))
                    {
                        Console.WriteLine($"  → [スキップ] 変換後の値が空");
                        continue;
                    }

                    // m_fixed_to_attr_mapから検索
                    var attrMap = attrMaps.FirstOrDefault(m => m.AttrCd == columnInfo.AttrCd);

                    ClProductAttr? productAttr = null;

                    if (attrMap != null)
                    {
                        Console.WriteLine($"  → ケース1: m_fixed_to_attr_mapに存在 (value_role={attrMap.ValueRole})");
                        // ケース1: m_fixed_to_attr_mapに存在する場合
                        productAttr = ProcessWithFixedMap(
                            batchId,
                            tempProduct,
                            columnInfo,
                            attrMap,
                            attrDefinitions,
                            processedColumns,
                            sourceRaw
                        );
                    }
                    else
                    {
                        Console.WriteLine($"  → ケース2: m_fixed_to_attr_mapに存在しない");
                        // ケース2: m_fixed_to_attr_mapに存在しない場合
                        productAttr = ProcessWithoutFixedMap(
                            batchId,
                            tempProduct,
                            columnInfo,
                            attrDefinitions,
                            sourceRaw
                        );
                    }

                    if (productAttr != null)
                    {
                        productAttrs.Add(productAttr);
                        Console.WriteLine($"  ✓ 属性追加成功: source_id={productAttr.SourceId}, source_label={productAttr.SourceLabel}");
                    }
                    else
                    {
                        Console.WriteLine($"  → [スキップ] 属性がnull");
                    }
                }

                Console.WriteLine($"属性処理完了: {productAttrs.Count}件");
                return productAttrs;
            }
            catch (Exception ex) when (ex is not IngestException)
            {
                throw new IngestException(
                    ErrorCodes.DB_ERROR,
                    $"属性処理中にエラーが発生しました: {ex.Message}",
                    ex,
                    recordRef: $"temp_row_id:{tempProduct.TempRowId}"
                );
            }
        }

        /// <summary>
        /// m_fixed_to_attr_mapを使用した属性処理
        /// value_role: ID_AND_LABEL, ID_ONLY, LABEL_ONLY に対応
        /// </summary>
        private ClProductAttr? ProcessWithFixedMap(
            string batchId,
            TempProductParsed tempProduct,
            ProcessedColumnInfo columnInfo,
            MFixedToAttrMap attrMap,
            List<MAttrDefinition> attrDefinitions,
            Dictionary<string, ProcessedColumnInfo> processedColumns,
            Dictionary<string, string> sourceRaw)
        {
            string? sourceIdValue = null;
            string? sourceLabelValue = null;

            // value_roleに基づいて値を取得
            if (attrMap.ValueRole == "ID_AND_LABEL")
            {
                // IDとLabelの両方を取得
                sourceIdValue = FindValueBySourceColumn(attrMap.SourceIdColumn, processedColumns);
                sourceLabelValue = FindValueBySourceColumn(attrMap.SourceLabelColumn, processedColumns);
            }
            else if (attrMap.ValueRole == "ID_ONLY")
            {
                // IDのみを取得
                sourceIdValue = FindValueBySourceColumn(attrMap.SourceIdColumn, processedColumns);
                sourceLabelValue = "";
            }
            else if (attrMap.ValueRole == "LABEL_ONLY")
            {
                // Labelのみを取得
                sourceIdValue = "";
                sourceLabelValue = FindValueBySourceColumn(attrMap.SourceLabelColumn, processedColumns);
            }

            // 値が空の場合はスキップ
            if (string.IsNullOrWhiteSpace(sourceIdValue) && string.IsNullOrWhiteSpace(sourceLabelValue))
            {
                Console.WriteLine($"[スキップ] attr_cd={columnInfo.AttrCd}: 値が空");
                return null;
            }

            // source_rawを構築
            var sourceRawDict = new Dictionary<string, string>();
            if (!string.IsNullOrEmpty(attrMap.SourceIdColumn) && !string.IsNullOrEmpty(sourceIdValue))
            {
                sourceRawDict[attrMap.SourceIdColumn] = sourceIdValue;
            }
            if (!string.IsNullOrEmpty(attrMap.SourceLabelColumn) && !string.IsNullOrEmpty(sourceLabelValue))
            {
                sourceRawDict[attrMap.SourceLabelColumn] = sourceLabelValue;
            }

            // data_typeを取得
            string? dataType = attrDefinitions.FirstOrDefault(ad => ad.AttrCd == columnInfo.AttrCd)?.DataType;

            // attr_seqを計算 (同じattr_cdの出現回数 + 1)
            short attrSeq = 1;

            var productAttr = new ClProductAttr
            {
                BatchId = batchId,
                TempRowId = tempProduct.TempRowId,
                AttrCd = columnInfo.AttrCd,
                AttrSeq = attrSeq,
                SourceId = sourceIdValue ?? "",
                SourceLabel = sourceLabelValue ?? "",
                SourceRaw = JsonSerializer.Serialize(sourceRawDict, new JsonSerializerOptions { WriteIndented = false }),
                DataType = dataType
            };

            Console.WriteLine($"[FixedMap] 属性追加: attr_cd={columnInfo.AttrCd}, value_role={attrMap.ValueRole}, source_id={sourceIdValue}, source_label={sourceLabelValue}");

            return productAttr;
        }

        /// <summary>
        /// m_fixed_to_attr_mapを使用しない属性処理
        /// </summary>
        private ClProductAttr? ProcessWithoutFixedMap(
            string batchId,
            TempProductParsed tempProduct,
            ProcessedColumnInfo columnInfo,
            List<MAttrDefinition> attrDefinitions,
            Dictionary<string, string> sourceRaw)
        {
            // 変換後の値を使用
            string? value = columnInfo.TransformedValue;

            if (string.IsNullOrWhiteSpace(value))
            {
                Console.WriteLine($"[スキップ] attr_cd={columnInfo.AttrCd}: 値が空");
                return null;
            }

            // data_typeを取得
            string? dataType = attrDefinitions.FirstOrDefault(ad => ad.AttrCd == columnInfo.AttrCd)?.DataType;

            // source_rawを構築
            var sourceRawDict = new Dictionary<string, string>();
            if (!string.IsNullOrEmpty(columnInfo.Header) && !string.IsNullOrEmpty(value))
            {
                // ヘッダー名をキーとして使用
                sourceRawDict[columnInfo.Header] = value;
            }

            // attr_seqを計算
            short attrSeq = 1;

            var productAttr = new ClProductAttr
            {
                BatchId = batchId,
                TempRowId = tempProduct.TempRowId,
                AttrCd = columnInfo.AttrCd,
                AttrSeq = attrSeq,
                SourceId = value,
                SourceLabel = "",
                SourceRaw = JsonSerializer.Serialize(sourceRawDict, new JsonSerializerOptions { WriteIndented = false }),
                DataType = dataType
            };

            Console.WriteLine($"[NoFixedMap] 属性追加: attr_cd={columnInfo.AttrCd}, source_id={value}");

            return productAttr;
        }

        /// <summary>
        /// source_columnからprocessed_columnsを検索して値を取得
        /// 例: source_brand_id → col_6の transformed_value を取得
        /// </summary>
        private string? FindValueBySourceColumn(string? sourceColumn, Dictionary<string, ProcessedColumnInfo> processedColumns)
        {
            if (string.IsNullOrEmpty(sourceColumn))
            {
                Console.WriteLine($"    [FindValue] sourceColumnが空");
                return null;
            }

            // target_columnがsourceColumnと一致する列を探す
            // 例: source_brand_id → brand_id
            string targetColumn = sourceColumn.Replace("source_", "");
            Console.WriteLine($"    [FindValue] sourceColumn={sourceColumn} → targetColumn={targetColumn}");

            foreach (var kvp in processedColumns.Values)
            {
                if (kvp.TargetColumn?.Equals(targetColumn, StringComparison.OrdinalIgnoreCase) == true)
                {
                    Console.WriteLine($"    [FindValue] ✓ 見つかった: target_column={kvp.TargetColumn}, transformed_value='{kvp.TransformedValue}'");
                    return kvp.TransformedValue;
                }
            }

            Console.WriteLine($"    [FindValue] × 見つからない: targetColumn={targetColumn}");
            return null;
        }
    }

    /// <summary>
    /// processed_columns内の列情報を表すクラス
    /// </summary>
    public class ProcessedColumnInfo
    {
        [System.Text.Json.Serialization.JsonPropertyName("csv_column_index")]
        public int CsvColumnIndex { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("header")]
        public string Header { get; set; } = string.Empty;

        [System.Text.Json.Serialization.JsonPropertyName("raw_value")]
        public string RawValue { get; set; } = string.Empty;

        [System.Text.Json.Serialization.JsonPropertyName("transformed_value")]
        public string TransformedValue { get; set; } = string.Empty;

        [System.Text.Json.Serialization.JsonPropertyName("target_column")]
        public string TargetColumn { get; set; } = string.Empty;

        [System.Text.Json.Serialization.JsonPropertyName("projection_kind")]
        public string ProjectionKind { get; set; } = string.Empty;

        [System.Text.Json.Serialization.JsonPropertyName("attr_cd")]
        public string AttrCd { get; set; } = string.Empty;

        [System.Text.Json.Serialization.JsonPropertyName("transform_expr")]
        public string TransformExpr { get; set; } = string.Empty;

        [System.Text.Json.Serialization.JsonPropertyName("is_required")]
        public bool IsRequired { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("is_injected")]
        public bool IsInjected { get; set; }

        [System.Text.Json.Serialization.JsonPropertyName("mapping_success")]
        public bool? MappingSuccess { get; set; }
    }
}
