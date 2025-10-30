using System.Text.Json;
using ProductDataIngestion.Models;

namespace ProductDataIngestion.Services
{
    /// <summary>
    /// extras_json からデータを取り出し、
    /// 商品属性（cl_product_attr）を作るクラスです。
    /// - processed_columns と source_raw を読みます
    /// - m_fixed_to_attr_map, m_attr_definitionを使います
    /// </summary>
    public class AttributeProcessor
    {
        private readonly DataImportService _dataService;

        /// <summary>
        /// コンストラクタです。
        /// DataImportService を受け取ります。
        /// </summary>
        /// <param name="dataService">DB参照用のサービス</param>
        public AttributeProcessor(DataImportService dataService)
        {
            _dataService = dataService;
        }

        /// <summary>
        /// extras_json の中の processed_columns を取り出します。
        /// 戻り値は列名（例: col_1）をキーにした辞書です。
        /// </summary>
        /// <param name="extrasJson">extras_json の文字列</param>
        /// <returns>列情報の辞書</returns>
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

                // processed_columns は { 列名: 列情報 } の形です
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
                    $"extras_json の解析に失敗しました: {ex.Message}",
                    ex
                );
            }
        }

        /// <summary>
        /// extras_json の中から source_raw を取り出します。
        /// 戻り値は列名->生データ の辞書です。
        /// </summary>
        /// <param name="extrasJson">extras_json の文字列</param>
        /// <returns>source_raw の辞書</returns>
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
                    $"source_raw の解析に失敗しました: {ex.Message}",
                    ex
                );
            }
        }

        /// <summary>
        /// 属性を作るメインの処理です。
        /// - processed_columns を読みます
        /// - 必要な列だけを選びます
        /// - マスタにより source_id や source_label を作ります
        /// </summary>
        /// <param name="batchId">バッチID</param>
        /// <param name="tempProduct">一時データの行</param>
        /// <param name="groupCompanyCd">グループ会社コード</param>
        /// <param name="dataKind">データ種別</param>
        /// <returns>作った ClProductAttr のリスト</returns>
        public async Task<List<ClProductAttr>> ProcessAttributesAsync(
            string batchId,
            TempProductParsed tempProduct,
            string groupCompanyCd,
            string dataKind)
        {
            var productAttrs = new List<ClProductAttr>();

            try
            {
                // extras_json から processed_columns と source_raw を取り出す
                Console.WriteLine($"\n=== extras_json 解析開始 (temp_row_id={tempProduct.TempRowId}) ===");
                var processedColumns = ExtractProcessedColumns(tempProduct.ExtrasJson);
                var sourceRaw = ExtractSourceRaw(tempProduct.ExtrasJson);

                // 最初の数件だけ確認用に表示
                Console.WriteLine($"✓ extras_json から抽出: processed_columns={processedColumns.Count}件, source_raw={sourceRaw.Count}件");
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

                // 
                var attrMaps = await _dataService.GetFixedToAttrMapsAsync(groupCompanyCd, dataKind);
                var attrDefinitions = await _dataService.GetAttrDefinitionsAsync();

                // 3) 必要な列だけ選ぶ（is_required=true && PRODUCT 系）
                var requiredColumns = processedColumns
                    .Where(kvp => kvp.Value.IsRequired &&
                                 (kvp.Value.ProjectionKind == "PRODUCT" || kvp.Value.ProjectionKind == "PRODUCT_EAV"))
                    .OrderBy(kvp => kvp.Value.CsvColumnIndex)
                    .ToList();

                Console.WriteLine($"\n✓ フィルタ後の処理対象列数: {requiredColumns.Count} (PRODUCT + PRODUCT_EAV)");

                // 4) 各列を処理
                foreach (var columnKvp in requiredColumns)
                {
                    var columnInfo = columnKvp.Value;

                    Console.WriteLine($"\n--- 処理中: {columnKvp.Key} ---");
                    Console.WriteLine($"  attr_cd={columnInfo.AttrCd}, header={columnInfo.Header}");
                    Console.WriteLine($"  transformed_value='{columnInfo.TransformedValue}'");

                    // attr_cd がないとエラー（必須列なので）
                    if (string.IsNullOrEmpty(columnInfo.AttrCd))
                    {
                        throw new IngestException(
                            ErrorCodes.MAPPING_NOT_FOUND,
                            $"必須列 {columnInfo.CsvColumnIndex} ({columnInfo.Header}) の attr_cd が m_data_import_d で定義されていません",
                            recordRef: $"temp_row_id:{tempProduct.TempRowId}"
                        );
                    }

                    // 変換後の値が空ならスキップ
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
                        // マップルールがあればそちらを使う
                        Console.WriteLine($"  → ケース1: m_fixed_to_attr_map にルールあり (value_role={attrMap.ValueRole})");
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
                        // 無ければ単純に transformed_value を使う
                        Console.WriteLine($"  → ケース2: m_fixed_to_attr_map にルールなし");
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
                        Console.WriteLine($"  → [スキップ] 属性が null");
                    }
                }

                Console.WriteLine($"属性処理完了: {productAttrs.Count} 件");
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
        /// m_fixed_to_attr_map のルールに従って source_id と source_label を作ります。
        /// value_role によって挙動が変わります。
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

            // value_role によって取り方を変える
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

            short attrSeq = 1; // 簡易実装: 出現回数計算は別で実装可能

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
        /// m_fixed_to_attr_map に対応しない通常の属性処理。
        /// columnInfo.TransformedValue を source_id として扱い、ClProductAttr を生成する。
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
    /// source_column 名（例: source_brand_id）から対応する processed_columns の transformed_value を探して返す。
    /// 内部的には "source_" プレフィックスを取り除いた target_column を比較して検索する。
    /// 見つからない場合は null を返す。
    /// </summary>
    /// <param name="sourceColumn">source_ で始まる列名</param>
    /// <param name="processedColumns">processed_columns の辞書</param>
    /// <returns>見つかった transformed_value または null</returns>
    private string? FindValueBySourceColumn(string? sourceColumn, Dictionary<string, ProcessedColumnInfo> processedColumns)
        {
            if (string.IsNullOrEmpty(sourceColumn))
            {
                Console.WriteLine($"    [FindValue] sourceColumn が空");
                return null;
            }

            // target_columnがsourceColumnと一致する列を探す
            // 例: source_brand_id → brand_id
            string targetColumn = sourceColumn.Replace("source_", "");
            Console.WriteLine($"    [FindValue] sourceColumn={sourceColumn} -> targetColumn={targetColumn}");

            foreach (var kvp in processedColumns.Values)
            {
                if (kvp.TargetColumn?.Equals(targetColumn, StringComparison.OrdinalIgnoreCase) == true)
                {
                    Console.WriteLine($"    [FindValue] ✓ 見つかった: target_column={kvp.TargetColumn}, transformed_value='{kvp.TransformedValue}'");
                    return kvp.TransformedValue;
                }
            }

            Console.WriteLine($"    [FindValue] 見つからない: targetColumn={targetColumn}");
            return null;
        }
    }

}
