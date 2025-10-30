using System.Text.Json;
using ProductDataIngestion.Models;

namespace ProductDataIngestion.Services
{
    /// <summary>
    /// extras_json から加工済みカラム情報を読み取り、cl_product_attr 相当の属性データを組み立てるクラス。
    /// - processed_columns: 前段の取り込み/変換結果（列ごとのヘッダ、変換後値、必須など）
    /// - source_raw: 生列データのスナップショット
    /// 固定→属性マッピング（m_fixed_to_attr_map）と属性定義（m_attr_definition）に従って、
    /// 取り込み段階（INGEST）の属性行（ClProductAttr）を生成します。
    /// </summary>
    public class AttributeProcessor
    {
        private readonly DataImportService _dataService;

        /// <summary>
        /// AttributeProcessor を生成します。
        /// </summary>
        /// <param name="dataService">インポート関連の参照データ取得サービス。</param>
        public AttributeProcessor(DataImportService dataService)
        {
            _dataService = dataService;
        }

        /// <summary>
        /// extras_json から processed_columns セクションを取り出して辞書化します。
        /// 列名（例: "col_1"）→ 列情報（ProcessedColumnInfo）のディクショナリを返します。
        /// </summary>
        /// <param name="extrasJson">Temp テーブルに格納されている extras_json。</param>
        /// <returns>列名をキーとする列情報の辞書（大文字小文字を区別しない）。</returns>
        /// <exception cref="IngestException">JSON 解析に失敗した場合。</exception>
        public Dictionary<string, ProcessedColumnInfo> ExtractProcessedColumns(string extrasJson)
        {
            if (string.IsNullOrWhiteSpace(extrasJson))
            {
                return new Dictionary<string, ProcessedColumnInfo>(StringComparer.OrdinalIgnoreCase);
            }

            try
            {
                // extras_json をルート辞書にデシリアライズ
                var extrasRoot = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(extrasJson);
                if (extrasRoot == null || !extrasRoot.TryGetValue("processed_columns", out var processedElement))
                {
                    return new Dictionary<string, ProcessedColumnInfo>(StringComparer.OrdinalIgnoreCase);
                }

                var result = new Dictionary<string, ProcessedColumnInfo>(StringComparer.OrdinalIgnoreCase);
                foreach (var property in processedElement.EnumerateObject())
                {
                    // 各要素を ProcessedColumnInfo にデシリアライズ
                    var columnInfo = JsonSerializer.Deserialize<ProcessedColumnInfo>(property.Value.GetRawText());
                    if (columnInfo != null)
                    {
                        result[property.Name] = columnInfo;
                    }
                }

                return result;
            }
            catch (Exception ex)
            {
                throw new IngestException(
                    ErrorCodes.PARSE_FAILED,
                    $"extras_json の processed_columns を解析できません: {ex.Message}",
                    ex);
            }
        }

        /// <summary>
        /// extras_json から source_raw セクションを取り出して辞書化します。
        /// 列ヘッダ名 → 生値のディクショナリ（存在しない場合は空辞書）。
        /// </summary>
        /// <param name="extrasJson">Temp テーブルに格納されている extras_json。</param>
        /// <returns>列ヘッダをキーとする生値ディクショナリ。</returns>
        /// <exception cref="IngestException">JSON 解析に失敗した場合。</exception>
        public Dictionary<string, string> ExtractSourceRaw(string extrasJson)
        {
            if (string.IsNullOrWhiteSpace(extrasJson))
            {
                return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            }

            try
            {
                // extras_json をルート辞書にデシリアライズ
                var extrasRoot = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(extrasJson);
                if (extrasRoot == null || !extrasRoot.TryGetValue("source_raw", out var rawElement))
                {
                    return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                }

                var sourceRaw = JsonSerializer.Deserialize<Dictionary<string, string>>(rawElement.GetRawText());
                return sourceRaw != null
                    ? new Dictionary<string, string>(sourceRaw, StringComparer.OrdinalIgnoreCase)
                    : new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            }
            catch (Exception ex)
            {
                throw new IngestException(
                    ErrorCodes.PARSE_FAILED,
                    $"extras_json の source_raw を解析できません: {ex.Message}",
                    ex);
            }
        }

        /// <summary>
        /// processed_columns とマスタ情報をもとに cl_product_attr 相当の属性リストを生成します。
        /// 実装ポイント:
        /// - アクティブなインポート設定から profile_id を推定（追跡用）。
        /// - m_fixed_to_attr_map を group_company_cd, projection_kind で取得し、
        ///   attr_cd ごとに priority（数値が小さいほど高優先）で最優先マップを選定。
        /// - 列が必須かつ対象投影（PRODUCT/PRODUCT_EAV）のみ処理。
        /// - マップあり: value_role に従い source_id/label を構成。
        /// - マップなし: transformed_value を利用し WARN で生成。
        /// </summary>
        /// <param name="batchId">バッチID。</param>
        /// <param name="tempProduct">一時商品の１レコード。</param>
        /// <param name="groupCompanyCd">グループ会社コード。</param>
        /// <param name="dataKind">データ種別（例: PRODUCT）。</param>
        /// <returns>生成された ClProductAttr のリスト。</returns>
        /// <exception cref="IngestException">必要マッピング未定義や解析失敗などのエラー。</exception>
        public async Task<List<ClProductAttr>> ProcessAttributesAsync(
            string batchId,
            TempProductParsed tempProduct,
            string groupCompanyCd,
            string dataKind)
        {
            var productAttrs = new List<ClProductAttr>();
            var warns = new List<WarnInfo>();

            try
            {
                // 1) 列情報（processed_columns）を抽出
                var processedColumns = ExtractProcessedColumns(tempProduct.ExtrasJson);

                long profileId = 0L;
                try
                {
                    // 2) アクティブな設定から profile_id を推定（provenance 用）
                    var active = await _dataService.GetActiveImportSettingsAsync(groupCompanyCd, dataKind);
                    if (active?.Count > 0)
                    {
                        profileId = active[0].ProfileId;
                    }
                }
                catch
                {
                    // 取得できなくても処理は継続（追跡情報のみ）。
                }

                // 3) 固定→属性マッピングを取得し、attr_cd 単位で最優先（priority 最小）を選ぶ
                // - PRODUCT と PRODUCT_EAV の両方のマッピングを取得して統合する
                var attrMapsPRODUCT = await _dataService.GetFixedToAttrMapsAsync(groupCompanyCd, "PRODUCT");
                var attrMapsPRODUCT_EAV = await _dataService.GetFixedToAttrMapsAsync(groupCompanyCd, "PRODUCT_EAV");

                // 両方のマッピングを結合
                var attrMaps = attrMapsPRODUCT.Concat(attrMapsPRODUCT_EAV).ToList();

                var attrMapLookup = attrMaps
                    .Where(m => m.IsActive)
                    // priority が小さいほど高優先度として採用（同値は map_id で安定化）
                    .GroupBy(m => m.AttrCd, StringComparer.OrdinalIgnoreCase)
                    .ToDictionary(
                        g => g.Key,
                        g => g.OrderBy(m => m.Priority).ThenBy(m => m.MapId).First(),
                        StringComparer.OrdinalIgnoreCase);

                // 4) 属性定義（data_type 等）を取得
                var attrDefinitions = await _dataService.GetAttrDefinitionsAsync();

                // 5) 必須列（PRODUCT / PRODUCT_EAV のみ）を対象とする
                // - is_required = true の列のみ処理
                // - projection_kind が PRODUCT または PRODUCT_EAV の列のみ対象（EVENT等は除外）
                var requiredColumns = processedColumns
                    .Where(kvp => kvp.Value.IsRequired &&
                           (string.Equals(kvp.Value.ProjectionKind, "PRODUCT", StringComparison.OrdinalIgnoreCase) ||
                            string.Equals(kvp.Value.ProjectionKind, "PRODUCT_EAV", StringComparison.OrdinalIgnoreCase)))
                    .OrderBy(kvp => kvp.Value.CsvColumnIndex)
                    .Select(kvp => kvp.Value)
                    .ToList();

                // 6) 列ごとに属性を組み立て
                foreach (var columnInfo in requiredColumns)
                {
                    if (string.IsNullOrWhiteSpace(columnInfo.AttrCd))
                    {
                        throw new IngestException(
                            ErrorCodes.MAPPING_NOT_FOUND,
                            $"attr_cd が未設定です: csv_column_index={columnInfo.CsvColumnIndex}, header='{columnInfo.Header}'",
                            recordRef: $"temp_row_id:{tempProduct.TempRowId}");
                    }

                    if (string.IsNullOrWhiteSpace(columnInfo.TransformedValue))
                    {
                        // 変換後値が空の場合はスキップ＋警告を収集
                        warns.Add(new WarnInfo
                        {
                            AttrCd = columnInfo.AttrCd,
                            WarnCode = "EMPTY_TRANSFORMED_VALUE",
                            Message = $"変換後の値が空です: attr_cd={columnInfo.AttrCd}",
                            TempRowId = tempProduct.TempRowId.ToString()
                        });
                        continue;
                    }

                    // 優先度で選ばれたマップを参照（なければ通常処理にフォールバック）
                    attrMapLookup.TryGetValue(columnInfo.AttrCd, out var attrMap);

                    ClProductAttr? productAttr = attrMap != null
                        ? ProcessWithFixedMap(batchId, tempProduct, columnInfo, attrMap, attrDefinitions, processedColumns)
                        : ProcessWithoutFixedMap(batchId, tempProduct, columnInfo, attrDefinitions);

                    if (productAttr == null)
                    {
                        continue;
                    }

                    if (attrMap != null)
                    {
                        // マップ経由で生成されたことを provenance に記録（追跡用）
                        productAttr.ProvenanceJson = JsonSerializer.Serialize(new
                        {
                            stage = "INGEST",
                            from = dataKind,
                            via = "fixed_map",
                            profile_id = profileId
                        });
                    }
                    else if (productAttr.QualityStatus.Equals("WARN", StringComparison.OrdinalIgnoreCase))
                    {
                        // マップなしで生成された WARN を集計
                        warns.Add(new WarnInfo
                        {
                            AttrCd = columnInfo.AttrCd,
                            WarnCode = "MAP_NOT_FOUND",
                            Message = $"No m_fixed_to_attr_map entry found for attr_cd: {columnInfo.AttrCd}",
                            TempRowId = tempProduct.TempRowId.ToString()
                        });
                    }

                    productAttrs.Add(productAttr);
                }

                if (warns.Count > 0)
                {
                    Console.WriteLine($"WARN: {warns.Count} 件の属性で追加警告 (batch:{batchId}, tempRow:{tempProduct.TempRowId})");
                }

                return productAttrs;
            }
            catch (Exception ex) when (ex is not IngestException)
            {
                throw new IngestException(
                    ErrorCodes.DB_ERROR,
                    $"属性生成に失敗しました: {ex.Message}",
                    ex,
                    recordRef: $"temp_row_id:{tempProduct.TempRowId}");
            }
        }

        /// <summary>
        /// m_fixed_to_attr_map のルールに基づいて、source_id / source_label を構成し ClProductAttr を生成します。
        /// value_role（ID_AND_LABEL / ID_ONLY / LABEL_ONLY）に応じて入力列の使い分けを行います。
        /// </summary>
        /// <param name="batchId">バッチID。</param>
        /// <param name="tempProduct">一時レコード。</param>
        /// <param name="columnInfo">列情報（attr_cd や transformed_value など）。</param>
        /// <param name="attrMap">選定済みの固定→属性マップ。</param>
        /// <param name="attrDefinitions">属性定義（data_type を参照）。</param>
        /// <param name="processedColumns">processed_columns の辞書。</param>
        /// <returns>作成された属性行。入力空値の場合は null。</returns>
        private ClProductAttr? ProcessWithFixedMap(
            string batchId,
            TempProductParsed tempProduct,
            ProcessedColumnInfo columnInfo,
            MFixedToAttrMap attrMap,
            List<MAttrDefinition> attrDefinitions,
            IReadOnlyDictionary<string, ProcessedColumnInfo> processedColumns)
        {
            string? sourceIdValue = null;
            string? sourceLabelValue = null;

            // value_role により、ID/Label のどちらをどの列から採用するかを分岐
            switch (attrMap.ValueRole?.ToUpperInvariant())
            {
                case "ID_AND_LABEL":
                    sourceIdValue = FindValueBySourceColumn(attrMap.SourceIdColumn, processedColumns)
                        ?? columnInfo.TransformedValue;
                    sourceLabelValue = FindValueBySourceColumn(attrMap.SourceLabelColumn, processedColumns);
                    break;

                case "ID_ONLY":
                    sourceIdValue = FindValueBySourceColumn(attrMap.SourceIdColumn, processedColumns)
                        ?? columnInfo.TransformedValue;
                    sourceLabelValue = string.Empty;
                    break;

                case "LABEL_ONLY":
                    sourceIdValue = string.Empty;
                    sourceLabelValue = FindValueBySourceColumn(attrMap.SourceLabelColumn, processedColumns)
                        ?? columnInfo.TransformedValue;
                    break;

                default:
                    sourceIdValue = columnInfo.TransformedValue;
                    sourceLabelValue = string.Empty;
                    break;
            }

            // どちらも空なら生成しない（スキップ）
            if (string.IsNullOrWhiteSpace(sourceIdValue) && string.IsNullOrWhiteSpace(sourceLabelValue))
            {
                return null;
            }

            // provenance 用の raw 断片（使用した列と値）をミニマムに記録
            var sourceRawDict = new Dictionary<string, string>();
            if (!string.IsNullOrWhiteSpace(attrMap.SourceIdColumn) && !string.IsNullOrWhiteSpace(sourceIdValue))
            {
                sourceRawDict[attrMap.SourceIdColumn] = sourceIdValue;
            }
            if (!string.IsNullOrWhiteSpace(attrMap.SourceLabelColumn) && !string.IsNullOrWhiteSpace(sourceLabelValue))
            {
                sourceRawDict[attrMap.SourceLabelColumn] = sourceLabelValue!;
            }

            // data_type は属性定義から引き当てる
            var dataType = attrDefinitions.FirstOrDefault(ad => ad.AttrCd == columnInfo.AttrCd)?.DataType;

            return new ClProductAttr
            {
                BatchId = batchId,
                TempRowId = tempProduct.TempRowId,
                AttrCd = columnInfo.AttrCd,
                AttrSeq = 1,
                SourceId = sourceIdValue ?? string.Empty,
                SourceLabel = sourceLabelValue ?? string.Empty,
                SourceRaw = JsonSerializer.Serialize(sourceRawDict),
                DataType = dataType,
                QualityStatus = "OK",
                QualityDetailJson = "{}"
            };
        }

        /// <summary>
        /// 固定→属性マップが見つからない場合のフォールバック生成。
        /// transformed_value を source_id として WARN で出力します。
        /// </summary>
        private ClProductAttr? ProcessWithoutFixedMap(
            string batchId,
            TempProductParsed tempProduct,
            ProcessedColumnInfo columnInfo,
            List<MAttrDefinition> attrDefinitions)
        {
            var value = columnInfo.TransformedValue;
            if (string.IsNullOrWhiteSpace(value))
            {
                return null;
            }

            // data_type は属性定義から引き当てる
            var dataType = attrDefinitions.FirstOrDefault(ad => ad.AttrCd == columnInfo.AttrCd)?.DataType;

            // provenance 用にヘッダと値を最小限で保持
            var sourceRawDict = new Dictionary<string, string>();
            if (!string.IsNullOrWhiteSpace(columnInfo.Header))
            {
                sourceRawDict[columnInfo.Header] = value;
            }

            var qualityDetail = new
            {
                warn_code = "MAP_NOT_FOUND",
                message = $"No m_fixed_to_attr_map entry found for attr_cd: {columnInfo.AttrCd}"
            };

            return new ClProductAttr
            {
                BatchId = batchId,
                TempRowId = tempProduct.TempRowId,
                AttrCd = columnInfo.AttrCd,
                AttrSeq = 1,
                SourceId = value,
                SourceLabel = string.Empty,
                SourceRaw = JsonSerializer.Serialize(sourceRawDict),
                DataType = dataType,
                QualityStatus = "WARN",
                QualityDetailJson = JsonSerializer.Serialize(qualityDetail)
            };
        }

        /// <summary>
        /// source_ プレフィックス付きの列名（例: source_brand_id）から、
        /// processed_columns 内の target_column を比較して transformed_value を探します。
        /// </summary>
        private static string? FindValueBySourceColumn(
            string? sourceColumn,
            IReadOnlyDictionary<string, ProcessedColumnInfo> processedColumns)
        {
            if (string.IsNullOrWhiteSpace(sourceColumn))
            {
                return null;
            }

            var targetColumn = sourceColumn.StartsWith("source_", StringComparison.OrdinalIgnoreCase)
                ? sourceColumn.Substring("source_".Length)
                : sourceColumn;

            foreach (var info in processedColumns.Values)
            {
                if (!string.IsNullOrWhiteSpace(info.TargetColumn) &&
                    string.Equals(info.TargetColumn, targetColumn, StringComparison.OrdinalIgnoreCase))
                {
                    return info.TransformedValue;
                }
            }

            return null;
        }
    }
}

