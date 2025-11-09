using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json;
using CsvHelper;
using CsvHelper.Configuration;
using ProductDataIngestion.Models;

namespace ProductDataIngestion.Services.Ingestion
{
    /// <summary>
    /// 【CsvTempIngestionService】
    /// CSVファイルの各行を読み取り、一時テーブル（temp_product_parsed / temp_product_event）向けの
    /// 中間データを生成するサービスクラス。
    /// 
    /// - IngestService から呼び出される下位処理。
    /// - ここでは1行ごとのCSV解析・検証・エラーハンドリングを担当する。
    /// </summary>
    public class CsvTempIngestionService
    {
        private readonly CsvValidator _csvValidator;// CSV構造（必須列・空行など）の検証担当
        private readonly DatabaseSchemaInspector _schemaInspector;// DBスキーマ確認（列のNOT NULLなどをチェック）
        /// <summary>
        /// コンストラクタ。Validator と SchemaInspector を依存注入で受け取る。
        /// </summary>
        public CsvTempIngestionService(CsvValidator csvValidator, DatabaseSchemaInspector schemaInspector)
        {
            _csvValidator = csvValidator ?? throw new ArgumentNullException(nameof(csvValidator));
            _schemaInspector = schemaInspector ?? throw new ArgumentNullException(nameof(schemaInspector));
        }

        /// <summary>
        /// CSVファイルを1行ずつ読み込み、
        /// temp_product_parsed / temp_product_event / record_error 用のリストを組み立てるメイン処理。
        /// 
        /// 【返却値】
        /// - readCount: 総読込行数
        /// - okCount: 成功件数
        /// - ngCount: エラー件数
        /// </summary>
        public async Task<(int readCount, int okCount, int ngCount)> ReadCsvAndSaveToTempAsync(
            string filePath, // CSVファイルパス
            string batchId, // バッチID（batch_runで採番された識別子）
            string groupCompanyCd, // グループ会社コード（KM/RKE/KBOなど）
            int headerRowIndex, // ヘッダー行の行番号（1行目=1）
            string targetEntity,
            List<MDataImportD> importDetails, // m_data_import_d の列定義・マッピングルール
            CsvConfiguration config, // CsvHelper用の設定（区切り文字・文字コードなど）
            List<TempProductParsed> tempProducts, // 商品temp格納リスト
            List<TempProductEvent> tempEvents, // イベントtemp格納リスト
            List<RecordError> recordErrors) // エラーレコード格納リスト
        {
            int readCount = 0, okCount = 0, ngCount = 0;

            try
            {
                // 1.ファイルエンコーディングを検証
                ValidateFileEncoding(filePath, config.Encoding ?? Encoding.UTF8);
                // 2.CSVリーダー初期化
                using var reader = new StreamReader(filePath, config.Encoding ?? Encoding.UTF8);
                using var csv = new CsvReader(reader, config);

                // 3. ヘッダー行のスキップ処理
                int skippedRows = 0;
                if (headerRowIndex > 1)
                {
                    for (int i = 1; i < headerRowIndex; i++)
                    {
                        if (!await csv.ReadAsync())
                        {
                            // 指定行まで到達できない＝ファイル破損
                            throw new IngestException(
                                ErrorCodes.PARSE_FAILED,
                                $"ヘッダー行 {headerRowIndex} に到達できません (ファイルの行数不足)"
                            );
                        }
                        skippedRows++;
                    }
                    Console.WriteLine($"ヘッダー行より前の {skippedRows} 行をスキップしました。");
                }
                // 4.ヘッダー行の読込と検証
                // 仕様変更: ヘッダー有無を区別せず、常にファイルの先頭行から列数を推定し、必須列チェックを行う
                string[] headers;
                int detectedColumnCount = 0;

                // ファイル先頭行を読み取って列数を推定
                var firstLine = File.ReadLines(filePath).FirstOrDefault();
                if (string.IsNullOrWhiteSpace(firstLine))
                {
                    throw new IngestException(
                        ErrorCodes.PARSE_FAILED,
                        "CSVファイルが空です。列数を判定できません。"
                    );
                }

                // 区切り文字（delimiter）で分割して列数を取得
                detectedColumnCount = firstLine.Split(config.Delimiter).Length;
                Console.WriteLine($"推定列数: {detectedColumnCount}（delimiter='{config.Delimiter}'）");

                // ヘッダー行を読むかどうかの設定がある場合、CsvHelperでも一応ヘッダーを読み取る
                if (config.HasHeaderRecord)
                {
                    if (!await csv.ReadAsync())
                    {
                        throw new IngestException(
                            ErrorCodes.MISSING_COLUMN,
                            $"ヘッダー行 (行番号:{headerRowIndex}) が読み込めませんでした。"
                        );
                    }

                    csv.ReadHeader();
                    headers = csv.HeaderRecord ?? Array.Empty<string>();
                    if (headers.Length == 0)
                    {
                        Console.WriteLine("⚠ ヘッダーが空のため、列名は自動推定されます。");
                        headers = Enumerable.Range(1, detectedColumnCount)
                                            .Select(i => $"column_{i}")
                                            .ToArray();
                    }
                    else
                    {
                        Console.WriteLine($"ヘッダー読込完了 (列数:{headers.Length})");
                    }
                }
                else
                {
                    // ヘッダー無し設定の場合 → ダミーヘッダーを作成
                    headers = Enumerable.Range(1, detectedColumnCount)
                                        .Select(i => $"column_{i}")
                                        .ToArray();
                    Console.WriteLine($"ヘッダー無しCSVとして処理 (列数:{detectedColumnCount})");
                }

                // 取込定義（importDetails）と推定列数を照合
                _csvValidator.ValidateColumnCount(importDetails, detectedColumnCount);

                // 5.データ行ループ処理
                long dataRowNumber = 0;// 論理行番号（ヘッダーを除くデータ行カウント）
                int currentPhysicalLine = headerRowIndex;// ファイル上の物理行位置

                while (await csv.ReadAsync())// 各行を順次読み込む
                {
                    currentPhysicalLine++;
                    dataRowNumber++;
                    readCount++;

                    var record = csv.Parser.Record;// CSV1行の全列データを配列で取得

                    try
                    {
                        // 空行・欠損行の検証
                        _csvValidator.ValidateEmptyRecord(record, dataRowNumber, currentPhysicalLine);
                        // CSV1行をTempProductまたはTempEventに変換
                        MapCsvRowToTempProduct(
                            batchId,
                            groupCompanyCd,
                            dataRowNumber,
                            currentPhysicalLine,
                            targetEntity,
                            record!,
                            headers,
                            importDetails,
                            tempProducts,
                            tempEvents,
                            recordErrors);

                        okCount++;// 正常行としてカウント
                    }
                    catch (IngestException ex)
                    {
                        // 想定内の業務エラー（例：必須項目なし・マッピング未定義など）
                        RecordIngestError(batchId, dataRowNumber, currentPhysicalLine, ex, record, recordErrors);
                        ngCount++;
                    }
                    catch (Exception ex)
                    {
                        // 想定外のエラーをIngestExceptionとして包んで処理統一
                        var ingestEx = new IngestException(
                            ErrorCodes.PARSE_FAILED,
                            $"行処理中に想定外のエラーが発生しました: {ex.Message}",
                            ex,
                            recordRef: $"line:{dataRowNumber}"
                        );
                        RecordIngestError(batchId, dataRowNumber, currentPhysicalLine, ingestEx, record, recordErrors);
                        ngCount++;
                    }
                }
                // 統計結果を返す
                return (readCount, okCount, ngCount);
            }
            catch (IngestException)
            {
                throw; // 業務エラーは上位へそのまま伝搬
            }
            catch (Exception ex)
            {
                // 全体のCSV解析中での致命的なエラー
                throw new IngestException(
                    ErrorCodes.PARSE_FAILED,
                    $"CSV読込処理で例外が発生しました: {ex.Message}",
                    ex
                );
            }
        }

        // 【MapCsvRowToTempProduct】
        // CSVの1行を、TempProductParsedまたはTempProductEventオブジェクトに変換する。
        // importDetailsのマッピング定義を参照し、列ごとの変換・検証を実施。
        private void MapCsvRowToTempProduct(
            string batchId,
            string groupCompanyCd,
            long dataRowNumber,
            int currentPhysicalLine,
            string targetEntity,
            string[] record,
            string[] headers,
            List<MDataImportD> importDetails,
            List<TempProductParsed> tempProducts,
            List<TempProductEvent> tempEvents,
            List<RecordError> recordErrors)
        {
                // 1. targetEntity に基づいてオブジェクト生成
                TempProductEvent? tempEvent = null;
                TempProductParsed? tempProduct = null;

                switch (targetEntity.ToUpperInvariant())
                {
                    case "PRODUCT":
                        tempProduct = new TempProductParsed
                        {
                            TempRowId = Guid.NewGuid(),
                            BatchId = batchId,
                            LineNo = dataRowNumber,
                            SourceGroupCompanyCd = groupCompanyCd,
                            StepStatus = "READY",
                            ExtrasJson = "{}"
                        };
                        break;

                    case "EVENT":
                        tempEvent = new TempProductEvent
                        {
                            TempRowEventId = Guid.NewGuid(),
                            BatchId = batchId,
                            TimeNo = dataRowNumber,
                            IdemKey = $"{batchId}:{dataRowNumber}",
                            SourceGroupCompanyCd = groupCompanyCd,
                            StepStatus = "READY",
                            ExtrasJson = "{}"
                        };
                        break;

                    default:
                        throw new IngestException(
                            ErrorCodes.INVALID_INPUT,
                            $"不明なtargetEntity: '{targetEntity}' (line {dataRowNumber}, physical line {currentPhysicalLine})",
                            recordRef: $"line:{dataRowNumber}",
                            rawFragment: string.Join(",", record)
                        );
                }

                bool isEventMode = targetEntity.Equals("EVENT", StringComparison.OrdinalIgnoreCase);

            // 2.列ごとの処理準備
            var extrasDict = new Dictionary<string, object>(); // 各列の加工後情報を保持
            var sourceRawDict = new Dictionary<string, string>(); // 元CSVデータ保持
            var requiredFieldErrors = new List<string>(); // 必須項目エラー収集

            bool storeIdSet = false; // 店舗ID重複防止
            bool storeNmSet = false; // 店舗名重複防止
             // importDetailsをcolumn_seq単位でグルーピング（1列に複数マッピング対応）
            var groupedDetails = importDetails
                .OrderBy(d => d.ColumnSeq)
                .ThenBy(d => d.ProjectionKind)
                .GroupBy(d => d.ColumnSeq)
                .ToDictionary(g => g.Key, g => g.ToList());
            // 3. 各列のマッピング処理
            foreach (var kvp in groupedDetails)
            {
                int colSeq = kvp.Key;
                var detailsForCol = kvp.Value;
                string? rawValue = null;
                string headerName = "N/A";
                bool isInjectedValue = colSeq == 0;// システム補完列（例：group_company_cd）
                // CSVデータから実際の値を取得
                if (colSeq == 0)
                {
                    rawValue = groupCompanyCd;
                    headerName = "[system:group_company_cd]";
                }
                else
                {
                    int csvIndex = colSeq - 1;
                    if (csvIndex >= headers.Length || csvIndex >= record.Length)
                    {
                        continue;// 列欠落
                    }
                    rawValue = record[csvIndex];
                    headerName = headers.Length > csvIndex ? headers[csvIndex] : $"column_{colSeq}";
                    sourceRawDict[headerName] = rawValue ?? string.Empty;
                }
                // マッピング定義が存在しない列はエラー
                if (!detailsForCol.Any())
                {
                    var profileId = importDetails.FirstOrDefault()?.ProfileId ?? 0L;
                    var noRuleEx = new IngestException(
                        ErrorCodes.MAPPING_NOT_FOUND,
                        $"列 {colSeq} に対応するマッピングが存在しません (profile_id:{profileId})",
                        recordRef: $"column:{colSeq}"
                    );
                    RecordIngestError(batchId, dataRowNumber, currentPhysicalLine, noRuleEx, record, recordErrors);
                    continue;
                }

                int subIndex = 0;
                 // 4. 同じ列に対する複数の変換ルール適用
                foreach (var detail in detailsForCol)
                {
                    // ★ transform_exprに基づいて値を加工（例："trim(@)"）
                    string? transformedValue = ApplyTransformExpression(rawValue, detail.TransformExpr ?? string.Empty);

                    bool mappingSuccess = false;
                    // PRODUCT用マッピング（列名に対応するプロパティへ値をセット）
                    if (!isEventMode &&
                        !string.IsNullOrEmpty(detail.TargetColumn) &&
                        string.Equals(detail.ProjectionKind, "PRODUCT", StringComparison.OrdinalIgnoreCase) &&
                        detail.IsRequired)
                    {
                        string propertyName = "source" + ConvertToPascalCase(detail.TargetColumn);
                        mappingSuccess = SetTempProductProperty(tempProduct!, propertyName, transformedValue);
                    }

                    if (isEventMode && tempEvent != null && string.Equals(detail.ProjectionKind, "EVENT", StringComparison.OrdinalIgnoreCase))
                    {
                        var attrCd = (detail.AttrCd ?? string.Empty).ToUpperInvariant();
                        var targetCol = (detail.TargetColumn ?? string.Empty).ToLowerInvariant();
                        switch (attrCd)
                        {
                            case "PRODUCT_CD":
                                if (string.IsNullOrWhiteSpace(tempEvent.SourceProductId)) tempEvent.SourceProductId = rawValue;
                                break;
                            case "NEW_USED_KBN":
                                if (string.IsNullOrWhiteSpace(tempEvent.SourceNewUsedKbnRaw)) tempEvent.SourceNewUsedKbnRaw = rawValue;
                                break;
                            case "EVENT_TS":
                                if (string.IsNullOrWhiteSpace(tempEvent.EventTsRaw)) tempEvent.EventTsRaw = rawValue;
                                break;
                            case "EVENT_KIND":
                                if (string.IsNullOrWhiteSpace(tempEvent.EventKindRaw)) tempEvent.EventKindRaw = rawValue;
                                break;
                            case "EVENT_QUANTITY":
                                if (string.IsNullOrWhiteSpace(tempEvent.QtyRaw)) tempEvent.QtyRaw = rawValue;
                                break;
                            case "STORE":
                                // 店舗関連列のマッピング（id/name自動識別）
                                if (targetCol.Contains("store_id") && !storeIdSet) { tempEvent.SourceStoreIdRaw = rawValue; storeIdSet = true; }
                                else if (targetCol.Contains("store_nm") && !storeNmSet) { tempEvent.SourceStoreNmRaw = rawValue; storeNmSet = true; }
                                else if (!storeIdSet) { tempEvent.SourceStoreIdRaw = rawValue; storeIdSet = true; }
                                else if (!storeNmSet) { tempEvent.SourceStoreNmRaw = rawValue; storeNmSet = true; }
                                break;
                            default:
                                break;
                        }
                    }
                    // 各列の処理情報をextras_json構成用に格納
                    string uniqueKey = string.IsNullOrEmpty(detail.AttrCd)
                        ? $"col_{colSeq}_sub{subIndex++}"
                        : $"col_{colSeq}_{detail.AttrCd.Replace(":", "_")}";

                    extrasDict[uniqueKey] = new
                    {
                        csv_column_index = colSeq,
                        header = headerName,
                        raw_value = rawValue ?? string.Empty,
                        transformed_value = transformedValue ?? string.Empty,
                        target_column = detail.TargetColumn ?? string.Empty,
                        projection_kind = detail.ProjectionKind,
                        attr_cd = detail.AttrCd ?? string.Empty,
                        transform_expr = detail.TransformExpr ?? string.Empty,
                        is_required = detail.IsRequired,
                        is_injected = isInjectedValue,
                        mapping_success = mappingSuccess
                    };
                    // 必須列で値が空の場合はエラー候補として追加
                    if (!string.IsNullOrEmpty(detail.TargetColumn) && string.IsNullOrWhiteSpace(transformedValue))
                    {
                        string tableName = detail.ProjectionKind?.ToUpperInvariant() switch
                        {
                            "PRODUCT" => "temp_product_parsed",
                            "EVENT" => "temp_product_event",
                            "PRODUCT_EAV" => "cl_product_attr",
                            _ => "temp_product_parsed"
                        };

                        string columnName = "source_" + detail.TargetColumn.ToLowerInvariant();
                        // DBスキーマ上、NOT NULL定義なら必須エラーとして記録
                        if (_schemaInspector.IsColumnNotNull(tableName, columnName))
                        {
                            requiredFieldErrors.Add($"列 {colSeq} ({detail.AttrCd}) は必須です (DB定義: {tableName}.{columnName} が NOT NULL)");
                        }
                    }
                }
            }
　　　　　　　// 5. 必須項目検証（収集済みエラーを評価）
            _csvValidator.ValidateRequiredFields(requiredFieldErrors, dataRowNumber, currentPhysicalLine);
            // 6. EVENTモード特有の検証（数量チェック）
            if (isEventMode && tempEvent != null)
            {
                var qtyRaw = tempEvent.QtyRaw;
                if (string.IsNullOrWhiteSpace(qtyRaw))
                {
                    throw new IngestException(
                        ErrorCodes.MISSING_COLUMN,
                        "EVENT_QUANTITY が未入力です",
                        recordRef: $"line:{dataRowNumber}");
                }
                else if (!decimal.TryParse(qtyRaw, out decimal qtyValue))
                {
                    throw new IngestException(
                        ErrorCodes.PARSE_FAILED,
                        $"EVENT_QUANTITY が数値ではありません: {qtyRaw}",
                        recordRef: $"line:{dataRowNumber}");
                }
                else if (qtyValue < 0)
                {
                    throw new IngestException(
                        ErrorCodes.PARSE_FAILED,
                        $"EVENT_QUANTITY が負の値です: {qtyValue}",
                        recordRef: $"line:{dataRowNumber}");
                }
            }
            // 7.extras_json生成（解析履歴をJSON化）
            var extrasJson = JsonSerializer.Serialize(new
            {
                source_raw = sourceRawDict,// 元CSV値
                processed_columns = extrasDict, // 各列の加工後情報
                csv_headers = headers,// CSVヘッダー
                physical_line = currentPhysicalLine,
                data_row_number = dataRowNumber,
                processing_timestamp = DateTime.UtcNow
            }, new JsonSerializerOptions { WriteIndented = false });
            // 8.tempリストへ追加（PRODUCT/EVENT分岐）
            if (isEventMode && tempEvent != null)
            {
                tempEvent.ExtrasJson = extrasJson;
                tempEvents.Add(tempEvent);
            }
            else if (tempProduct != null)
            {
                tempProduct.ExtrasJson = extrasJson;
                tempProducts.Add(tempProduct);
            }
        }

        // 【RecordIngestError】
        // 行単位のエラー発生時に record_error 用のオブジェクトを作成し、リストに追加する。
        private void RecordIngestError(
            string batchId,
            long dataRowNumber,
            int currentPhysicalLine,
            IngestException ex,
            string[]? record,
            List<RecordError> recordErrors)
        {
            string rawFragment = ex.RawFragment ?? string.Empty;
            // ★ RawFragment（元データ断片）が指定されていない場合は、CSV行全体を連結して保持する
            if (string.IsNullOrEmpty(rawFragment))
            {
                rawFragment = record != null ? string.Join(",", record) : string.Empty;
            }

            var error = new RecordError
            {
                BatchId = batchId,// バッチ識別子
                Step = "INGEST",// 現在の処理ステップ
                RecordRef = !string.IsNullOrEmpty(ex.RecordRef) ? ex.RecordRef : $"line:{dataRowNumber}", // 対象行を一意に識別する参照情報
                ErrorCd = ex.ErrorCode,// エラーコード（定義済みEnum）
                ErrorDetail = $"データ行 {dataRowNumber} (物理行 {currentPhysicalLine}): {ex.Message}",// 詳細エラーメッセージ
                RawFragment = rawFragment// 問題行の生データ
            };
            // コンソール出力でログにも残す
            Console.WriteLine($"エラー記録: [{error.ErrorCd}] {error.ErrorDetail}");
            // リストに追加 → 上位サービスで一括DB保存される
            recordErrors.Add(error);
        }

        // 【ApplyTransformExpression】
        // transform_expr の定義内容に従って、列値を加工する。
        // 例）trim(@), upper(@), to_timestamp(@,'YYYYMMDD')
        private string? ApplyTransformExpression(string? value, string transformExpr)
        {
            if (value == null) return null;

            string? result = value;
            // 変換式が未設定の場合 → trimだけ実施（全角/半角スペース除去）
            if (string.IsNullOrEmpty(transformExpr))
            {
                return value.Trim().Trim('\u3000');
            }
            // 複数式を「,」「;」で分割して順次適用
            var transformations = transformExpr.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries);

            foreach (var transformation in transformations)
            {
                var expr = transformation.Trim();
                // trim(@): 前後スペース削除
                if (expr.Equals("trim(@)", StringComparison.OrdinalIgnoreCase))
                {
                    if (result != null)
                    {
                        result = result.Trim().Trim('\u3000');
                    }
                }
                // upper(@): 英字を大文字化
                else if (expr.Equals("upper(@)", StringComparison.OrdinalIgnoreCase))
                {
                    if (result != null)
                    {
                        result = result.ToUpperInvariant();
                    }
                }
                else if (expr.StartsWith("nullif(@", StringComparison.OrdinalIgnoreCase))
                {
                    if (string.IsNullOrWhiteSpace(result))
                    {
                        result = null;
                    }
                }
                // to_timestamp(@,'YYYYMMDD'): PostgreSQL形式の日付変換式を評価
                else if (expr.StartsWith("to_timestamp(@", StringComparison.OrdinalIgnoreCase))
                {
                    if (!string.IsNullOrWhiteSpace(result))
                    {
                        result = ParseDateExpression(result, expr);
                    }
                }
            }

            return result;
        }
        // 【ParseDateExpression】
        // PostgreSQLの to_timestamp(@,'YYYYMMDD') 形式を解析して、.NET側DateTimeに変換。
        private string? ParseDateExpression(string value, string expression)
        {
            try
            {
                // to_timestamp(@,'YYYYMMDD') のフォーマット部分を抽出
                var match = System.Text.RegularExpressions.Regex.Match(
                    expression,
                    @"to_timestamp\(@\s*,\s*'([^']+)'\)",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase
                );

                if (!match.Success)
                {
                    Console.WriteLine($"[警告] 日付フォーマットの解析に失敗しました: {expression}");
                    return value;// フォーマット不明 → 元値を返す
                }

                var formatPattern = match.Groups[1].Value;
                var dotNetFormat = ConvertPostgreSqlFormatToDotNet(formatPattern);
                // "YYYYMMDD" → "yyyyMMdd" に変換して .NET側で解析
                if (DateOnly.TryParseExact(value.Trim(), dotNetFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dateOnly))
                {
                    return dateOnly.ToString("yyyy-MM-dd");
                }
                else if (DateTime.TryParseExact(value.Trim(), dotNetFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dateTime))
                {
                    return dateTime.ToString("yyyy-MM-dd");
                }
                else
                {
                    Console.WriteLine($"[警告] 日付のパースに失敗しました: value='{value}', format='{dotNetFormat}'");
                    return value;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[エラー] 日付変換中に例外が発生しました: {ex.Message}");
                return value;
            }
        }

        // 【ConvertPostgreSqlFormatToDotNet】
        // PostgreSQLの日時フォーマットを .NET用のフォーマットに置換。
        // "YYYY-MM-DD" → "yyyy-MM-dd"
        private string ConvertPostgreSqlFormatToDotNet(string pgFormat)
        {
            var conversions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                { "YYYY", "yyyy" },
                { "YY", "yy" },
                { "MM", "MM" },
                { "DD", "dd" },
                { "HH24", "HH" },
                { "HH12", "hh" },
                { "MI", "mm" },
                { "SS", "ss" },
                { "MS", "fff" }
            };

            string result = pgFormat;

            foreach (var kvp in conversions)
            {
                // 正規表現で置換（大文字小文字区別なし）
                result = System.Text.RegularExpressions.Regex.Replace(
                    result,
                    kvp.Key,
                    kvp.Value,
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase
                );
            }

            return result;
        }

        // 【SetTempProductProperty】
        // リフレクションを用いて TempProductParsed のプロパティに値を設定する。
        // "sourceBrandNm" 等のプロパティ名に動的にアクセスする。
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
                // 大文字小文字を無視して再検索（例：sourcebrandnm など）
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
        
        // 【ConvertToPascalCase】
        // "brand_nm" → "BrandNm" に変換。
        private string ConvertToPascalCase(string input)
        {
            if (string.IsNullOrEmpty(input)) return input;

            var parts = input.Split(new[] { '_', '-' }, StringSplitOptions.RemoveEmptyEntries);
            return string.Concat(parts.Select(part =>
                char.ToUpperInvariant(part[0]) + part.Substring(1).ToLowerInvariant()));
        }

        // 【ValidateFileEncoding】
        // CSVファイルの実際のエンコーディングと、設定値が一致するか検証する。
        // 不一致の場合は INVALID_ENCODING エラーを投げる。
        private void ValidateFileEncoding(string filePath, Encoding expectedEncoding)
        {
            try
            {
                Console.WriteLine("\n--- 文字コード検証 ---");
                Console.WriteLine($"想定エンコーディング: {expectedEncoding.EncodingName} ({expectedEncoding.WebName})");

                const int sampleSize = 4096;// ファイル冒頭の4KBで検出
                byte[] sampleBytes;
                // ファイル先頭部分を読み取ってBOM確認
                using (var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    int bytesToRead = (int)Math.Min(sampleSize, fs.Length);
                    sampleBytes = new byte[bytesToRead];
                    fs.Read(sampleBytes, 0, bytesToRead);
                }
                // BOMから実際のエンコーディングを推定
                string detectedEncodingName = DetectEncodingFromBOM(sampleBytes);
                if (!string.IsNullOrEmpty(detectedEncodingName))
                {
                    Console.WriteLine($"BOM から検出したエンコーディング: {detectedEncodingName}");
                    // 設定エンコーディングと異なる場合は例外
                    if (!IsEncodingCompatible(detectedEncodingName, expectedEncoding))
                    {
                        throw new IngestException(
                            ErrorCodes.INVALID_ENCODING,
                            $"ファイルのエンコーディング ({detectedEncodingName}) が設定 ({expectedEncoding.WebName}) と一致しません。"
                        );
                    }
                }
                else
                {
                    // BOMなし → 設定エンコードで続行
                    Console.WriteLine("BOM が見つからないため、想定エンコーディングで読み込みを継続します。");
                }
            }
            catch (IngestException)
            {
                throw; // 明示的な業務エラーはそのまま再スロー
            }
            catch (Exception ex)
            {
                // 例外を捕捉しINVALID_ENCODINGエラーに変換
                throw new IngestException(
                    ErrorCodes.INVALID_ENCODING,
                    $"文字コード検証でエラーが発生しました: {ex.Message}",
                    ex
                );
            }
        }
        
        // 【DetectEncodingFromBOM】
        // BOM(バイトオーダーマーク)から実際の文字コードを判定する。
        private string DetectEncodingFromBOM(byte[] bytes)
        {
            if (bytes.Length >= 3 && bytes[0] == 0xEF && bytes[1] == 0xBB && bytes[2] == 0xBF)
                return "UTF-8";
            if (bytes.Length >= 2 && bytes[0] == 0xFF && bytes[1] == 0xFE)
                return "UTF-16LE";
            if (bytes.Length >= 2 && bytes[0] == 0xFE && bytes[1] == 0xFF)
                return "UTF-16BE";
            if (bytes.Length >= 4 && bytes[0] == 0xFF && bytes[1] == 0xFE && bytes[2] == 0x00 && bytes[3] == 0x00)
                return "UTF-32LE";
            if (bytes.Length >= 4 && bytes[0] == 0x00 && bytes[1] == 0x00 && bytes[2] == 0xFE && bytes[3] == 0xFF)
                return "UTF-32BE";

            return string.Empty;// BOMなし
        }

        // 【IsEncodingCompatible】
        // 検出された文字コードが、設定値と互換性があるかどうかを判定。
        private bool IsEncodingCompatible(string detectedName, Encoding expectedEncoding)
        {
            string expectedName = expectedEncoding.WebName.ToUpperInvariant();
            string detected = detectedName.ToUpperInvariant();
            // UTF-8系ならOK
            if (detected.Contains("UTF-8") || detected.Contains("UTF8"))
                return expectedName.Contains("UTF-8") || expectedName.Contains("UTF8");
            // UTF-16/UNICODE系も互換扱い
            if (detected.Contains("UTF-16"))
                return expectedName.Contains("UTF-16") || expectedName.Contains("UNICODE");
            // その他は完全一致を要求
            return detected == expectedName;
        }

    }
}
