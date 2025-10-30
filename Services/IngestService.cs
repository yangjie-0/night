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
    /// CSVファイル取込サービス
    /// 処理全体の流れ：
    /// 1. CSVファイルを受け取り、バッチ処理を開始
    /// 2. 取込ルールをDBから取得
    /// 3. CSV設定を構築してファイルを読む
    /// 4. データ変換と一時テーブル保存
    /// 5. 属性データを生成して本テーブルへ保存
    /// 6. 結果をバッチ単位で記録
    /// 
    /// このクラスは、CSVからDBへ安全にデータを取り込む中心的な役割を持つ。
    /// 主な機能：
    /// - CSVデータの取込と検証
    /// - 属性データの生成と保存
    /// - エラー処理とログ記録
    /// - バッチ処理の状態管理
    /// </summary>
    public class IngestService
    {
        // ===== フィールド定義 =====
        private readonly DataImportService _dataService; // データインポート設定を扱うサービス。m_data_import_setting や m_data_import_d を取得する。
        private readonly IBatchRepository _batchRepository; // バッチ処理（batch_runテーブル）を扱うリポジトリ。処理開始・終了の記録を行う。
        private readonly IProductRepository _productRepository; // 商品関連データを扱うリポジトリ。cl_product_attr や一時テーブルを操作。
        private readonly IDataImportRepository _dataRepository; // インポート設定情報（ルール・マッピング）の取得を行うリポジトリ。
        private readonly CsvValidator _csvValidator; // CSVの内容や形式をチェックするためのクラス（列数・必須項目・空行などを検証する）。
        private readonly AttributeProcessor _attributeProcessor; // CSV行から属性データを生成・変換するためのクラス。
        private readonly string _connectionString; // データベース接続文字列。全てのリポジトリがこの接続情報を共有して使用する。
        // ===== 処理中データの一時保持領域 =====
        private readonly List<BatchRun> _batchRuns = new(); // 現在の実行中または完了したバッチ処理の一覧を保持。
        private readonly List<TempProductParsed> _tempProducts = new(); // CSV解析後の一時商品データ（変換済み）を格納。
        private readonly List<RecordError> _recordErrors = new(); // CSV処理中に発生した行単位のエラー情報を格納。

        // ===== コンストラクタ =====        
        public IngestService(
            string connectionString,
            IBatchRepository batchRepository,
            IProductRepository productRepository)
        {
            // 接続文字列が null の場合は例外を出す
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            // バッチリポジトリが未設定なら例外
            _batchRepository = batchRepository ?? throw new ArgumentNullException(nameof(batchRepository));
            // 商品リポジトリが未設定なら例外
            _productRepository = productRepository ?? throw new ArgumentNullException(nameof(productRepository));
            // DataImportRepository（設定情報取得用）を内部で初期化
            _dataRepository = new DataImportRepository(_connectionString);
            // DataImportService（業務ロジック層）を生成
            _dataService = new DataImportService(_dataRepository);
            // CSV検証ツールを初期化（列名・必須項目チェックなどに使用）
            _csvValidator = new CsvValidator();
            // 属性変換処理クラスを初期化（tempデータ → EAV形式データの変換を担当）
            _attributeProcessor = new AttributeProcessor(_dataService);
        }

        /// <summary>
        /// CSVファイルを1件ずつ取り込み、EAV形式データを生成するメイン処理。
        /// 
        /// このメソッドは取込処理全体の「入口」となる部分で、
        /// バッチ開始 → 設定取得 → CSV解析 → 一時保存 → 属性生成 → 統計更新  の一連の流れを制御します。
        /// 処理ステップ:
        /// 1. バッチ処理の開始（一意のバッチIDを生成）
        /// 2. ファイル取込ルール（マッピング情報）の取得
        /// 3. CSVファイルの読込および必須項目検証
        /// 5. 一時テーブルへのデータ保存
        /// 6. データ（cl_product_attr）への変換保存
        /// 7. 処理結果の統計更新
        /// 
        /// 戻り値: バッチID（（後続処理・監査・再実行時の識別に使用））
        /// </summary>
        public async Task<string> ProcessCsvFileAsync(string filePath, string groupCompanyCd, string targetEntity)
        {
            Console.WriteLine($"=== CSV取込開始 ===\nファイル: {filePath}\nGP会社: {groupCompanyCd}\n処理モード: {targetEntity}");

            // 会社コード検証
            await ValidateCompanyCodeAsync(groupCompanyCd); // 指定された会社コードが有効かを検証する

            //  ▼ フロー1: バッチ起票（バッチ開始レコードを作成）
            var batchId = await CreateBatchRunAsync(filePath, groupCompanyCd, targetEntity);

            try
            {
                // ▼ フロー2: ファイル取込ルール取得（マッピング設定を取得）
                var (importSetting, importDetails) = await FetchImportRulesAsync(groupCompanyCd, targetEntity);

                // ▼ フロー3: CSV読込設定の初期化（区切り文字・エンコードなど）
                var (config, headerRowIndex) = ConfigureCsvReaderSettings(importSetting);

                // ▼ フロー4〜6: CSVデータを1行ずつ読込 → 検証 → 一時保存
                var result = await ReadCsvAndSaveToTempAsync(filePath, batchId, groupCompanyCd,
                                                             headerRowIndex, importDetails, config);

                // ▼ フロー7〜9: 一時データをもとに属性データを生成 → DB保存
                await GenerateProductAttributesAsync(batchId, groupCompanyCd, targetEntity);

                // ▼ フロー10: 処理件数・結果統計をバッチ単位で更新
                await UpdateBatchStatisticsAsync(batchId, result);

                Console.WriteLine($"=== 取込完了 ===\n読込: {result.readCount}\n成功: {result.okCount}\n失敗: {result.ngCount}");
                return batchId;
            }
            catch (IngestException ex)
            {
                // 設計上想定されるエラー（例：マッピング欠如など）
                await RecordErrorAndMarkBatchFailed(batchId, ex);
                throw; // フレームワークに例外を伝わらせる
            }
            catch (Exception ex)
            {
                // 想定外の例外（DB障害など）
                var dbError = new IngestException(
                    ErrorCodes.DB_ERROR,
                    $"予期しないエラーが発生しました: {ex.Message}",
                    ex
                );
                await RecordErrorAndMarkBatchFailed(batchId, dbError);
                throw dbError;
            }
        }

        #region フロー1: バッチ起票

        /// <summary>
        /// フロー1: バッチ起票
        /// 
        /// このメソッドは、CSV取込処理ごとに新しい「バッチ処理単位」を登録します。
        /// すべての取込実行は1つのbatch_idで管理され、
        /// 後続のエラー記録・統計更新・再実行管理の基礎となります。
        /// 
        /// - batch_idの生成（日時＋GUID）
        /// -冪等性キー（idem_key）作成（ファイル名＋最終更新時刻）
        /// - ステータス初期化（RUNNING）
        /// - started_at = now()
        /// - batch_runテーブルへの登録
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
                    BatchId = batchId,// バッチ識別ID
                    IdemKey = idemKey,// 冪等性キー（同一ファイル再取込防止）
                    GroupCompanyCd = groupCompanyCd,// 会社コード
                    DataKind = targetEntity,// 対象種別（PRODUCTなど）
                    FileKey = filePath,// ファイルパス
                    BatchStatus = "RUNNING",// 実行中
                    StartedAt = DateTime.UtcNow,// 開始時刻
                    CountsJson = "{\"INGEST\":{\"read\":0,\"ok\":0,\"ng\":0}}"// 初期統計値
                };

                await _batchRepository.CreateBatchRunAsync(batchRun);// DB登録
                _batchRuns.Add(batchRun);// メモリ上にキャッシュ（後続で更新する）

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
        /// 
        /// このメソッドでは、指定された会社コードと対象データ種別から
        /// 「どの列をどの属性にマッピングするか」という設定情報をDBから取得します。
        /// 
        /// 主な処理内容：
        /// - 入力: group_company_cd と target_entity
        /// - m_data_import_setting から有効な設定を検索（is_active=true）
        /// - 対応する profile_id の詳細定義（m_data_import_d）を全件取得
        /// - ルール不在/重複は致命的エラー → FAILED
        /// 
        /// 戻り値:
        ///  (取込設定オブジェクト, 列マッピングリスト)
        /// </summary>
        private async Task<(MDataImportSetting, List<MDataImportD>)> FetchImportRulesAsync(
            string groupCompanyCd, string targetEntity)
        {
            try
            {
                string usageNm = $"{groupCompanyCd}-{targetEntity}";// 用途キー（例: KM-PRODUCT）
                var importSetting = await _dataService.GetImportSettingAsync(groupCompanyCd, usageNm);

                // is_active チェック
                if (importSetting == null || !importSetting.IsActive)
                {
                    throw new IngestException(
                        ErrorCodes.MAPPING_NOT_FOUND,
                        $"有効なファイル取込設定が見つかりません: {usageNm}"
                    );
                }

                // 列マッピングの全件取得
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
        /// CSV読み込み用の設定を構築して返す
        /// 
        /// このメソッドでは、CSVHelperライブラリで使用する `CsvConfiguration` オブジェクトを作成する。
        /// 設定値は DB の `m_data_import_setting` テーブルに登録された情報（区切り文字、文字コードなど）を使用。
        /// 
        /// 主な設定内容:
        /// - HasHeaderRecord: CSVにヘッダー行が存在するかどうか
        /// - Delimiter: 区切り文字
        /// - BadDataFound: 不正データ行をスキップするための設定
        /// - MissingFieldFound: 列数不足の場合に例外を出さずスキップ
        /// - Encoding: ファイルの文字コード（UTF-8 / Shift_JIS / EUC-JP）
        /// 出力:
        /// - CsvConfiguration（CSVHelper用設定）
        /// - ヘッダー行番号（HeaderRowIndex）
        /// 
        /// - 設定取得時にエラーが発生した場合、INVALID_ENCODING エラーとして例外をスローする。
        /// </summary>
        private (CsvConfiguration, int) ConfigureCsvReaderSettings(MDataImportSetting importSetting)
        {
            try
            {
                var config = new CsvConfiguration(CultureInfo.InvariantCulture)
                {
                    HasHeaderRecord = true,// CSVの最初の行をヘッダー行として扱う
                    Delimiter = importSetting.Delimiter ?? ",", // 区切り文字（指定なければカンマ）
                    BadDataFound = context => { }, // 不正データ（例：改行や区切り不一致）を無視
                    MissingFieldFound = null, // 列不足でも例外を発生させない
                    Encoding = GetEncodingFromCharacterCode(importSetting.CharacterCd ?? "UTF-8")// ファイルの文字コード設定
                };
                // 戻り値としてCsvConfigurationとヘッダー行位置を返す
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
        /// SVファイルを1行ずつ読み込み、検証・変換を行い、一時テーブル(temp)に保存する。
        /// 
        /// この関数はCSV取込の中核であり、以下の処理を順に行う：
        ///
        /// 1. ヘッダー行の取得
        ///    - HeaderRowIndexで指定された行をスキップし、ヘッダー名を確定する。
        ///    - ヘッダーが空または不正な場合は PARSE_FAILED エラーを返す。
        /// 
        /// 2. データ行の処理  
        ///    - CSVの各行を順次読み込み、CsvValidatorで空行チェック・必須チェックを行う。
        ///    - 列ごとに importDetails のマッピング設定に従って変換する（transform_expr適用）。
        ///    - 変換結果と元データを extras_json に保持。
        /// 
        /// 3. 一時テーブルへの保存
        ///    - TempProductParsed オブジェクトとして一時メモリに格納し
        ///    - 最後にまとめて SaveToTempTablesAsync() でDBへ一括保存。
        /// 
        /// エラー処理：
        /// - PARSE_FAILED: CSV構造・形式不正
        /// - MISSING_COLUMN: 必須列が欠落 
        /// - INVALID_FORMAT: 型変換エラーなど 
        /// 
        /// 戻り値：読込件数・成功件数・失敗件数（タプル形式）
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

                // フロー4: ヘッダー行のスキップと取得
                for (int i = 1; i < headerRowIndex; i++)
                {
                    if (!await csv.ReadAsync())
                    {
                        throw new IngestException(
                            ErrorCodes.PARSE_FAILED,
                            $"ヘッダー行 {headerRowIndex} に到達できません"
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

                csv.ReadHeader();//その行を「列名リスト」として認識
                var headers = csv.HeaderRecord;//ヘッダー行の文字列配列を取得
                if (headers == null || headers.Length == 0)
                {
                    throw new IngestException(
                        ErrorCodes.PARSE_FAILED,
                        "ヘッダー行が空です"
                    );
                }

                Console.WriteLine($"ヘッダー取得完了: {headers.Length} 列");

                // 列マッピング検証（設定通りか確認）
                _csvValidator.ValidateColumnMappings(importDetails, headers);

                // データ行の読込開始
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
                        // 空行・空列チェック (CsvValidatorを使用)
                        _csvValidator.ValidateEmptyRecord(record, dataRowNumber, currentPhysicalLine);

                        // 各行をTempProductParsedに変換
                        MapCsvRowToTempProduct(batchId, groupCompanyCd, dataRowNumber, currentPhysicalLine,
                                               record!, headers, importDetails);
                        okCount++;
                    }
                    catch (IngestException ex)
                    {
                        // エラー（欠項目など）を記録してスキップ
                        RecordIngestError(batchId, dataRowNumber, currentPhysicalLine, ex, record);
                        ngCount++;
                    }
                    catch (Exception ex)
                    {
                        // 想定外の例外（例: 数値変換失敗など）
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
        /// CSV行を TempProductParsed に変換し、必要な検証を行う。
        /// 
        /// この関数は1行のCSVデータをマッピング定義（importDetails）に従って
        /// (TempProductParsed) に変換する役割を持つ。
        /// 
        /// 主な処理内容：
        /// - importDetails に従い 各列の値取得（CSV列 or 注入列）
        /// - column_seq=0 は注入値（GP会社コード）として扱う
        /// - column_seq > 0: CSV列番号 (1始まり、配列インデックスは -1 が必要)
        /// - transform_expr に基づく値変換（trim / upper / nullif など）
        /// - 必須項目チェック（is_required）
        /// - extras_json 作成（生データ＋変換結果＋ルール情報）
        /// 
        /// - CSVの値を変換・検証した後、TempProductParsed に格納
        /// - 欠項目やマッピングエラーは RecordIngestError に登録し処理を継続
        /// </summary>
        private void MapCsvRowToTempProduct(
                string batchId, string groupCompanyCd, long dataRowNumber, int currentPhysicalLine,
                string[] record, string[] headers, List<MDataImportD> importDetails)
        {
            // ▼ このCSV行を表す中間オブジェクト（後でtempテーブルに格納）
            var tempProduct = new TempProductParsed
            {
                TempRowId = Guid.NewGuid(),// 行ごとの一意ID（temp用）
                BatchId = batchId,// 紐づくバッチID
                LineNo = dataRowNumber,// データ行の連番（ヘッダー除く）
                SourceGroupCompanyCd = groupCompanyCd,// どの会社のデータか
                StepStatus = "READY",  // 現時点の行ステータス（後段で更新する想定）
                ExtrasJson = "{}" // 後で変換・検証の詳細を書き込む
            };

            // ▼ extras_json に格納する「処理詳細」の作業用辞書
            var extrasDict = new Dictionary<string, object>();// 列ごとの変換・マッピング結果を格納
            var sourceRawDict = new Dictionary<string, string>();// CSVの「生値」をヘッダー名でバックアップ
            var requiredFieldErrors = new List<string>(); // 必須チェックの未充足メッセージを一時保持

            // ▼ 列マッピング定義を column_seq 単位でまとめる
            var groupedDetails = importDetails
                .OrderBy(d => d.ColumnSeq)
                .ThenBy(d => d.ProjectionKind)  // PRODUCT → PRODUCT_EAV の順で処理
                .GroupBy(d => d.ColumnSeq)
                .ToDictionary(g => g.Key, g => g.ToList());

            // ▼ すべての column_seq を順に処理
            // - colSeq=0 は「注入列」（CSVからではなく固定値：会社コード）
            // - colSeq>0 は CSV の実列（1始まり → 配列indexは-1する）
            foreach (var kvp in groupedDetails)
            {
                int colSeq = kvp.Key;// の列番号（1始まり、0は注入）
                var detailsForCol = kvp.Value;  // 該列の全ルール（複数可）
                string? rawValue = null;// CSVから受け取った生値（または注入値）
                string headerName = "N/A";//CSVに存在しない列（注入列）の場合は "N/A"（該当なし）として扱われます。
                bool isInjectedValue = (colSeq == 0);

                if (colSeq == 0)
                {
                    // 注入列: 会社コード固定値
                    rawValue = groupCompanyCd;
                    headerName = "[注入:group_company_cd]";// デバッグ時に「CSV由来ではない」ことが分かるように明示
                }
                else
                {
                    // CSV列: 範囲チェック
                    int csvIndex = colSeq - 1;// 1始まり → 0始まりへ
                    if (csvIndex >= headers.Length || csvIndex >= record.Length)
                    {
                        continue;  // マッピングはあるが、CSVに列が無い（可用性のため例外にせずスキップ）
                    }
                    rawValue = record[csvIndex];// 該当セルの生値
                    headerName = headers[csvIndex]; // 対応ヘッダー名

                    // ▼ 生値のバックアップ（後でエラートレース・再処理の参考にする）
                    //- CSVに存在する列のみを保存（注入列は保存しない）
                    string backupKey = headerName;
                    sourceRawDict[backupKey] = rawValue ?? "";
                }

                // ▼ マッピング定義が空＝設計漏れ（致命的）だが、1行単位の記録にして継続
                if (!detailsForCol.Any())
                {
                    var profileId = importDetails.FirstOrDefault()?.ProfileId ?? 0L; // プロファイルID取得
                    var noRuleEx = new IngestException(
                        ErrorCodes.MAPPING_NOT_FOUND,
                        $"列 {colSeq} の取込ルールが見つかりません（profile_id: {profileId}）",
                        recordRef: $"column:{colSeq}"
                    );
                    // 行エラーとして記録し、この列の処理は飛ばす（他列は続ける）
                    RecordIngestError(batchId, dataRowNumber, currentPhysicalLine, noRuleEx, record);
                    continue;
                }

                // ▼ 同一列に複数ルールがあり得るため、1ルールずつ独立して変換・検証を行う
                int subIndex = 0;
                foreach (var detail in detailsForCol)
                {
                    // ▼ transform_expr を順に適用（trim / upper / nullif / 日付など）
                    string? transformedValue = ApplyTransformExpression(rawValue, detail.TransformExpr ?? "");

                    // ▼ PRODUCTの固定カラムに直接反映するケース
                    bool mappingSuccess = false;
                    if (!string.IsNullOrEmpty(detail.TargetColumn) &&
                        detail.ProjectionKind == "PRODUCT" &&
                        detail.IsRequired)
                    {
                        string propertyName = "source" + ConvertToPascalCase(detail.TargetColumn);
                        mappingSuccess = SetTempProductProperty(tempProduct, propertyName, transformedValue);
                    }

                    // ▼ extras_json に保存するキーを生成（重複防止のためユニーク化）
                    //    - AttrCd がない場合は subIndex 連番で埋める
                    //    - AttrCd がある場合は "col_{colSeq}_{ATTR_CD}" 形式（":" は "_" に）
                    string uniqueKey = string.IsNullOrEmpty(detail.AttrCd)
                        ? $"col_{colSeq}_sub{subIndex++}"
                        : $"col_{colSeq}_{detail.AttrCd.Replace(":", "_")}";  // e.g., "col_0_GROUP_COMPANY_CD"
                                                                              // ▼ 後追い調査に必要な情報を余さず保存
                                                                              //    - 生値・変換後・どのターゲット列に投影したか・必須か・成功したか等
                    extrasDict[uniqueKey] = new
                    {
                        csv_column_index = colSeq,// 設計上の列番号（1始まり）
                        header = headerName,// ヘッダー名（注入列は専用ラベル）
                        raw_value = rawValue ?? "", // CSVの元の値（または注入値）
                        transformed_value = transformedValue ?? "",// 変換後の値（nullは空文字に）
                        target_column = detail.TargetColumn ?? "",// マッピング先のカラム名（固定カラムの場合）
                        projection_kind = detail.ProjectionKind,// PRODUCT / PRODUCT_EAV など
                        attr_cd = detail.AttrCd ?? "", // 項目コード
                        transform_expr = detail.TransformExpr ?? "",// 適用した変換式
                        is_required = detail.IsRequired,// 必須フラグ
                        is_injected = false,// 注入列フラグ（現状falseで固定）
                        mapping_success = mappingSuccess// 固定カラムに反映できたか
                    };

                    // 必須チェック（true時のみ）
                    // - 変換後の値が空（null/空白）なら記録
                    if (detail.IsRequired && string.IsNullOrWhiteSpace(transformedValue))
                    {
                        requiredFieldErrors.Add($"列 {colSeq} ({detail.AttrCd}): 必須項目空");
                    }
                }
            }

            // 必須チェック結果 (CsvValidatorを使用)
            //   - まとめて1回例外化して RecordIngestError 側に回す設計（ReadCsvAndSaveToTempAsync で捕捉）
            _csvValidator.ValidateRequiredFields(requiredFieldErrors, dataRowNumber, currentPhysicalLine);

            // ▼ 変換のトレース性を高めるため、extras_json に「生値」「処理詳細」「メタ情報」をまとめる
            tempProduct.ExtrasJson = JsonSerializer.Serialize(new
            {
                source_raw = sourceRawDict,// CSV由来の生データ（ヘッダー名→値）
                processed_columns = extrasDict, // 列ごとの処理結果（変換後・マッピング先など）
                csv_headers = headers,// CSVヘッダーの配列（列順の再現に使える）
                physical_line = currentPhysicalLine,// ファイル上の物理行番号（デバッグ用）
                data_row_number = dataRowNumber,// データ行の通し番号（ヘッダー除く）
                processing_timestamp = DateTime.UtcNow// いつ処理したか（UTC）
            }, new JsonSerializerOptions { WriteIndented = false });

            // ▼ メモリ上の作業バッファに追加（のちに SaveToTempTablesAsync で一括DB保存）
            _tempProducts.Add(tempProduct);
        }

        #endregion

        #region フロー7-9: extras_jsonからデータ取得 → 属性生成 → cl_product_attr保存

        /// <summary>
        /// cl_product_attr作成
        /// TempProductParsed の extras_json を元に属性(ClProductAttr)を生成し、DBに保存する
        /// 
        /// 処理の流れ：
        /// 取込済みの TempProductParsed（CSV1行ごとの中間結果）を順番に処理
        /// 各行の extras_json を AttributeProcessor に渡し、EAV形式の属性へ変換
        /// 生成された ClProductAttr（属性レコード）をまとめてDBに保存
        /// </summary>
        private async Task GenerateProductAttributesAsync(
            string batchId, string groupCompanyCd, string dataKind)
        {
            try
            {
                // ▼ 生成された全商品の属性データを一時的に保存するリスト
                var allProductAttrs = new List<ClProductAttr>();
                // ▼ TempProductParsed リストに格納された各行（＝CSV1行分の解析結果）を処理
                foreach (var tempProduct in _tempProducts)
                {
                    // AttributeProcessorを使用して、extras_jsonの内容を解析・属性化
                    var productAttrs = await _attributeProcessor.ProcessAttributesAsync(
                        batchId,
                        tempProduct,
                        groupCompanyCd,
                        dataKind
                    );
                    // ▼ 生成された属性をリストに追加
                    allProductAttrs.AddRange(productAttrs);
                }

                // ▼ すべての属性をDBに保存（cl_product_attr テーブル）
                await _productRepository.SaveProductAttributesAsync(allProductAttrs);
                Console.WriteLine($"cl_product_attr保存完了: {allProductAttrs.Count} レコード");
            }
            catch (IngestException)
            {
                throw;
            }
            catch (Exception ex)
            {
                // 想定外のエラーは DB_ERROR として再スロー
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
    /// バッチ統計（read/ok/ng）を更新し、バッチ状態を設定する
    /// 
    /// 処理内容:
    /// バッチIDに紐づくBatchRunレコードをメモリから取得
    /// JSON形式で統計（読込件数・成功件数・失敗件数）をCountsJsonに保存
    /// エラーがあれば "PARTIAL"、すべて成功なら "SUCCESS" として状態更新
    /// 更新時刻(ended_at)を現在時刻に設定し、DB反映
    /// 
    /// この処理により、管理画面や監査ログで「この取込は何件成功／失敗したか」を確認可能。
    /// </summary>
    private async Task UpdateBatchStatisticsAsync(string batchId, (int readCount, int okCount, int ngCount) result)
        {
            // ▼ バッチIDに対応する実行情報を取得
            try
            {
                var batchRun = _batchRuns.FirstOrDefault(b => b.BatchId == batchId);
                if (batchRun != null)
                {
                    // ▼ 各ステップの件数情報をJSONとして格納
                    batchRun.CountsJson = JsonSerializer.Serialize(new
                    {
                        INGEST = new { read = result.readCount, ok = result.okCount, ng = result.ngCount },
                        CLEANSE = new { },
                        UPSERT = new { },
                        CATALOG = new { }
                    });
                    // ▼ 失敗件数があれば PARTIAL、なければ SUCCESS
                    batchRun.BatchStatus = result.ngCount > 0 ? "PARTIAL" : "SUCCESS";
                    batchRun.EndedAt = DateTime.UtcNow;// 処理終了時刻を記録
                    // ▼ DB更新（batch_run テーブル）
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
        /// <summary>
        /// Ingest処理中に発生した行単位のエラーを記録
        /// 
        /// 処理内容:
        /// - 例外(IngestException)をRecordErrorに変換
        /// - コンソールに簡易ログを出力（開発・デバッグ用）
        /// - _recordErrorsリストに追加（後で一括DB保存）
        /// 
        /// - 例外は投げずに他の行の処理を継続する（まとめて DB に保存する設計）
        /// </summary>
        private void RecordIngestError(string batchId, long dataRowNumber, int currentPhysicalLine,
                    IngestException ex, string[]? record)
        {
            var error = new RecordError
            {
                BatchId = batchId,// どのバッチの処理か
                Step = "INGEST",// 現在の処理段階（取込ステップ）
                RecordRef = !string.IsNullOrEmpty(ex.RecordRef) ? ex.RecordRef : $"line:{dataRowNumber}",
                ErrorCd = ex.ErrorCode,// エラーコード（例: MAPPING_NOT_FOUND）
                ErrorDetail = $"データ行 {dataRowNumber} (物理行 {currentPhysicalLine}): {ex.Message}",
                RawFragment = !string.IsNullOrEmpty(ex.RawFragment)
                    ? ex.RawFragment
                    : string.Join(",", record?.Take(5) ?? Array.Empty<string>())// 行データの一部を保存
            };

            Console.WriteLine($"エラーレコード: [{error.ErrorCd}] {error.ErrorDetail}");
            _recordErrors.Add(error);
        }

        /// <summary>
    /// <summary>
    ///  致命的なIngest例外発生時の処理。
    /// 
    /// /// 処理内容:
    /// - RecordErrorを作成して_internalリストに追加
    /// - DBにエラーを一括保存
    /// - 該当バッチをFAILEDに更新（MarkBatchAsFailedAsyncを呼ぶ）
    /// 
    /// 入力:
    /// - batchId: 対象バッチID
    /// - ex: 発生した IngestException（個別の重大エラー）
    /// 
    /// - 保存中に再びエラーが起きても、上位でcatchしてログ出力のみ行う設計。
    /// </summary>
    private async Task RecordErrorAndMarkBatchFailed(string batchId, IngestException ex)
        {
            // エラーをrecord_errorテーブルに記録
            var error = new RecordError
            {
                BatchId = batchId,
                Step = "INGEST",
                RecordRef = ex.RecordRef ?? string.Empty,
                ErrorCd = ex.ErrorCode,
                ErrorDetail = ex.Message,
                RawFragment = ex.RawFragment ?? string.Empty
            };
            // ▼ まとめてrecord_errorテーブルに保存
            _recordErrors.Add(error);
            await _productRepository.SaveRecordErrorsAsync(_recordErrors);

            // ▼ バッチ全体を失敗状態に変更
            await MarkBatchAsFailedAsync(batchId, ex.Message);
        }

        /// <summary>
        /// <summary>
        /// バッチをFAILED状態としてマークする
        /// 
        /// 処理内容:
        /// - バッチ実行情報(BatchRun)を取得
        /// - 状態をFAILEDに変更し、終了時刻を現在時刻で更新
        /// - DBに反映
        /// 
        /// - この処理自体でエラーが起きた場合は握りつぶし、ログ出力のみ行う。
        /// </summary>
        private async Task MarkBatchAsFailedAsync(string batchId, string errorMessage)
        {
            try
            {
                var batchRun = _batchRuns.FirstOrDefault(b => b.BatchId == batchId);
                if (batchRun != null)
                {
                    batchRun.BatchStatus = "FAILED";// ステータスを失敗に変更
                    batchRun.EndedAt = DateTime.UtcNow;// 終了時間を現在時刻に
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
        /// フロー6: temp テーブルへのデータ保存処理
        /// 
        /// 処理内容:
        /// メモリ上に保持していた中間データ（_tempProducts, _recordErrors）をDBに保存
        /// ProductRepositoryを介して insert を実行
        /// 保存完了後、件数をコンソール出力
        /// </summary>
        private async Task SaveToTempTablesAsync()
        {
            try
            {
                // ▼ 商品データ（TempProductParsed）をtempテーブルへ保存
                await _productRepository.SaveTempProductsAsync(_tempProducts);
                // ▼ 行エラー情報（RecordError）も同時に保存
                await _productRepository.SaveRecordErrorsAsync(_recordErrors);
                // ▼ 保存件数をログに出力    
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
        /// transform_expr データ変換式の適用
        /// 
        /// 対応している変換処理：
        /// 1. スペース処理
        ///    - trim(@): 前後の半角・全角スペースを削除
        ///    
        /// 2. 文字変換
        ///    - upper(@): アルファベットを大文字に変換
        ///    
        /// 3. NULL処理
        ///    - nullif(@,''): 空文字列をNULLに変換
        ///    
        /// 4. 日付変換
        ///    - to_timestamp(@,'YYYY-MM-DD'): 文字列を日付形式に変換
        ///    
        /// 注意事項：
        /// - 変換失敗時は元の値を返す
        /// - 複数の変換をカンマで連結可能
        /// - 未指定時は自動でtrim(@)を適用
        /// </summary>
        private string? ApplyTransformExpression(string? value, string transformExpr)
        {
            // null 入力はそのまま返す
            if (value == null) return null;

            string? result = value;

            // ▼ transform_expr が空 → デフォルトでtrim処理だけ適用
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
                // 2. upper(@): 英字を大文字化
                else if (expr.Equals("upper(@)", StringComparison.OrdinalIgnoreCase))
                {
                    if (result != null)
                    {
                        result = result.ToUpper(); // すべての文字を大文字に
                    }
                }
                // 3. nullif(@,''): 空文字ならnullへ変換
                else if (expr.StartsWith("nullif(@", StringComparison.OrdinalIgnoreCase))
                {
                    if (string.IsNullOrWhiteSpace(result))
                    {
                        result = null; // 空文字または空白のみの場合は null に変換
                    }
                }
                // 4. to_timestamp(@,'YYYY-MM-DD'): 日付文字列をフォーマット変換
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
                // ▼ 正規表現で日付フォーマットを抽出
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

                var formatPattern = match.Groups[1].Value; // 抽出した "YYYY-MM-DD"

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
        ///TempProductParsed オブジェクトのプロパティに値を動的に設定
        /// 
        /// 処理内容:
        /// - Reflectionを使って指定プロパティ(propertyName)に値をセット
        /// - 大文字小文字を無視して再検索するフォールバック付き
        /// - 存在しないプロパティならfalseを返す
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
                 // 大文字小文字を無視して再検索（柔軟性確保）
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
        /// snake_case → PascalCase に変換
        ///
        /// 処理例:
        ///   "product_name" → "ProductName"
        ///   "group_company_cd" → "GroupCompanyCd"
        ///
        /// ※ .NETのプロパティ命名規則に合わせるための補助メソッド。
        /// </summary>
        private string ConvertToPascalCase(string input)
        {
            if (string.IsNullOrEmpty(input)) return input;

            var parts = input.Split(new char[] { '_', '-' }, StringSplitOptions.RemoveEmptyEntries);
            return string.Concat(parts.Select(part =>
                char.ToUpperInvariant(part[0]) + part.Substring(1).ToLowerInvariant()));
        }

        /// <summary>
        /// 処理内容:
        /// - m_companyテーブルから該当コードを検索
        /// - is_active=true の会社のみ有効
        /// - 無効または存在しない場合は IngestException(MAPPING_NOT_FOUND) をスロー
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
        /// GP会社コードの簡易検証（DB接続できない場合の代替）
        ///
        /// 有効コード:
        ///  - KM
        ///  - RKE
        ///  - KBO
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
