using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Linq;
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
        private readonly List<TempProductEvent> _tempEvents = new(); // CSV解析後の一時イベントデータ（生値中心）を格納。
        private readonly List<RecordError> _recordErrors = new(); // CSV処理中に発生した行単位のエラー情報を格納。
        // ===== データベーススキーマ情報キャッシュ =====
        private Dictionary<string, HashSet<string>>? _notNullColumnsCache; // テーブル名 -> NOT NULL列名のセット

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

            // データベーススキーマ情報の読込（NOT NULL制約チェック用）
            await LoadNotNullColumnsAsync();

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
                // 指定 group_company_cd / target_entity に対するアクティブ設定を全件取得
                var candidates = await _dataService.GetActiveImportSettingsAsync(groupCompanyCd, targetEntity);

                if (candidates == null || candidates.Count == 0)
                {
                    throw new IngestException(
                        ErrorCodes.MAPPING_NOT_FOUND,
                        $"有効なファイル取込設定が見つかりません: group_company_cd={groupCompanyCd}, target_entity={targetEntity}"
                    );
                }

                if (candidates.Count > 1)
                {
                    var ids = string.Join(",", candidates.Select(c => c.ProfileId));
                    throw new IngestException(
                        ErrorCodes.MAPPING_NOT_FOUND,
                        $"アクティブな取込設定が複数存在します: group_company_cd={groupCompanyCd}, target_entity={targetEntity}, profiles=[{ids}]"
                    );
                }

                var importSetting = candidates[0];

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
                    HasHeaderRecord = (importSetting.HeaderRowIndex > 0),// CSVの最初の行をヘッダー行として扱う
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
        ///    - HeaderRowIndexまで行をスキップし、ヘッダー名を確定する。
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
                // ★ 文字コード検証: CSVファイルの実際のエンコーディングと設定が一致するか確認
                ValidateFileEncoding(filePath, config.Encoding ?? Encoding.UTF8);

                using var reader = new StreamReader(filePath, config.Encoding ?? Encoding.UTF8);
                using var csv = new CsvReader(reader, config);

                // フロー4: ヘッダー行の取得（ヘッダー行より前の無効行をスキップ）
                // 注意: headerRowIndex=1 の場合、1行目がヘッダーなのでスキップ不要
                //       headerRowIndex=3 の場合、1行目と2行目をスキップし、3行目をヘッダーとして読む
                int skippedRows = 0;
                if (headerRowIndex > 1)
                {
                    // ヘッダー行より前の無効行（タイトル行や空行など）をスキップ
                    for (int i = 1; i < headerRowIndex; i++)
                    {
                        if (!await csv.ReadAsync())
                        {
                            throw new IngestException(
                                ErrorCodes.PARSE_FAILED,
                                $"ヘッダー行 {headerRowIndex} に到達できません（ファイルの行数が不足しています）"
                            );
                        }
                        skippedRows++;
                    }
                    Console.WriteLine($"ヘッダー行より前の{skippedRows}行をスキップしました");
                }

                string[] headers;
                if (headerRowIndex > 0)
                {
                    // ヘッダーありの場合: ヘッダーを読み込み、検証する
                    if (!await csv.ReadAsync())
                    {
                        throw new IngestException(
                            ErrorCodes.MISSING_COLUMN,
                            $"ヘッダー行（{headerRowIndex}行目）が読み込めません。ファイルが空か、行数が不足しています。"
                        );
                    }

                    csv.ReadHeader();
                    headers = csv.HeaderRecord;
                    if (headers == null || headers.Length == 0)
                    {
                        throw new IngestException(
                            ErrorCodes.MISSING_COLUMN,
                            $"ヘッダー行（{headerRowIndex}行目）が空です。必須列の検証ができません。"
                        );
                    }

                    Console.WriteLine($"ヘッダー取得完了（{headerRowIndex}行目）: {headers.Length} 列");

                    // 列マッピング検証（設定通りか確認）
                    _csvValidator.ValidateColumnMappings(importDetails, headers);
                }
                else
                {
                    // ヘッダーなしの場合: 検証をスキップし、空のヘッダー配列を設定
                    headers = Array.Empty<string>();
                    Console.WriteLine("ヘッダーなし設定のため、ヘッダーの読み込みと列マッピング検証をスキップします。");
                }

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
        ///
        /// ★注意: PRODUCT と EVENT が混在する場合の処理
        /// - m_data_import_d.projection_kind によって行ごとに分岐
        /// - PRODUCT / PRODUCT_EAV → temp_product_parsed へ保存（現在の実装）
        /// - EVENT → temp_product_event へ保存（未実装）
        /// - 同一ファイル内で projection_kind が混在する場合、行ごとに振り分けが必要
        /// </summary>
        private void MapCsvRowToTempProduct(
                string batchId, string groupCompanyCd, long dataRowNumber, int currentPhysicalLine,
                string[] record, string[] headers, List<MDataImportD> importDetails)
        {
            // ▼ このCSV行を表す中間オブジェクト（後でtempテーブルに格納）
            // 注意: 現在は PRODUCT 専用の TempProductParsed を使用
            // TODO: EVENT の場合は TempProductEvent モデルを使用する必要がある
            // EVENTモードかを判定し、必要ならイベント用の中間オブジェクトを準備
            bool isEventMode = importDetails.Any(d => string.Equals(d.ProjectionKind, "EVENT", StringComparison.OrdinalIgnoreCase));
            TempProductEvent? tempEvent = null;
            if (isEventMode)
            {
                tempEvent = new TempProductEvent
                {
                    TempRowEventId = Guid.NewGuid(),
                    BatchId = batchId,
                    TimeNo = dataRowNumber,  // DB列名: time_no
                    IdemKey = $"{batchId}:{dataRowNumber}",  // 冪等キー生成
                    SourceGroupCompanyCd = groupCompanyCd,
                    StepStatus = "READY",
                    ExtrasJson = "{}"
                };
            }
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

            // ▼ EVENT の STORE 列追跡用（同一 attr_cd で複数列がある場合の制御）
            bool storeIdSet = false;  // STORE の1列目（ID）が既に設定されたか
            bool storeNmSet = false;  // STORE の2列目（NM）が既に設定されたか

            // ▼ 列マッピング定義を column_seq 単位でまとめる
            // ★projection_kind によって処理順序を制御（PRODUCT → PRODUCT_EAV → EVENT）
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

                    // ▼ EVENT の固定カラムマッピング（m_data_import_d に基づく）
                    // *_raw フィールドには元のCSV値（trim前）を保存
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
                                // STORE は複数列の場合があるため、列番号順に id / nm を割り振る
                                // target_column に "store_id" または "store_nm" が含まれるかで判定
                                if (targetCol.Contains("store_id") && !storeIdSet) { tempEvent.SourceStoreIdRaw = rawValue; storeIdSet = true; }
                                else if (targetCol.Contains("store_nm") && !storeNmSet) { tempEvent.SourceStoreNmRaw = rawValue; storeNmSet = true; }
                                else if (!storeIdSet) { tempEvent.SourceStoreIdRaw = rawValue; storeIdSet = true; }
                                else if (!storeNmSet) { tempEvent.SourceStoreNmRaw = rawValue; storeNmSet = true; }
                                break;
                            default:
                                break;
                        }
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
                        projection_kind = detail.ProjectionKind,// PRODUCT / PRODUCT_EAV など（PRODUCT_EAV もここに入る！）
                        attr_cd = detail.AttrCd ?? "", // 項目コード
                        transform_expr = detail.TransformExpr ?? "",// 適用した変換式
                        is_required = detail.IsRequired,// 必須フラグ
                        is_injected = false,// 注入列フラグ（現状falseで固定）
                        mapping_success = mappingSuccess// 固定カラムに反映できたか
                    };

                    // 必須チェック: データベースのNOT NULL制約に基づいて検証
                    // - target_columnが指定されている場合は、そのカラムのNOT NULL制約をチェック
                    // - 値が空（null/空白）で、かつDBでNOT NULLの場合のみエラー
                    if (!string.IsNullOrEmpty(detail.TargetColumn) && string.IsNullOrWhiteSpace(transformedValue))
                    {
                        // projection_kindに応じてテーブルを特定
                        string tableName = detail.ProjectionKind?.ToUpperInvariant() switch
                        {
                            "PRODUCT" => "temp_product_parsed",
                            "EVENT" => "temp_product_event",
                            "PRODUCT_EAV" => "cl_product_attr",
                            _ => "temp_product_parsed"
                        };

                        // source_プレフィックスを付けた列名をチェック
                        string columnName = "source_" + detail.TargetColumn.ToLowerInvariant();

                        // データベースでNOT NULL制約がある場合のみエラー
                        if (IsColumnNotNull(tableName, columnName))
                        {
                            requiredFieldErrors.Add($"列 {colSeq} ({detail.AttrCd}): 必須項目空 (DB制約: {tableName}.{columnName} は NOT NULL)");
                        }
                    }
                }
            }

            // 必須チェック結果 (CsvValidatorを使用)
            //   - まとめて1回例外化して RecordIngestError 側に回す設計（ReadCsvAndSaveToTempAsync で捕捉）
            _csvValidator.ValidateRequiredFields(requiredFieldErrors, dataRowNumber, currentPhysicalLine);

            // ▼ EVENT専用の追加検証: qty_raw は 空/非数値/ゼロ未満 は行エラー
            if (isEventMode && tempEvent != null)
            {
                var qtyRaw = tempEvent.QtyRaw;
                if (string.IsNullOrWhiteSpace(qtyRaw))
                {
                    throw new IngestException(
                        ErrorCodes.MISSING_COLUMN,
                        "EVENT_QUANTITY が空です",
                        recordRef: $"行:{dataRowNumber}");
                }
                else if (!decimal.TryParse(qtyRaw, out decimal qtyValue))
                {
                    throw new IngestException(
                        ErrorCodes.PARSE_FAILED,
                        $"EVENT_QUANTITY が数値ではありません: {qtyRaw}",
                        recordRef: $"行:{dataRowNumber}");
                }
                else if (qtyValue < 0)
                {
                    throw new IngestException(
                        ErrorCodes.PARSE_FAILED,
                        $"EVENT_QUANTITY がゼロ未満です: {qtyValue}",
                        recordRef: $"行:{dataRowNumber}");
                }
            }

            // ▼ 変換のトレース性を高めるため、extras_json に「生値」「処理詳細」「メタ情報」をまとめる
            var extrasJson = JsonSerializer.Serialize(new
            {
                source_raw = sourceRawDict,
                processed_columns = extrasDict,
                csv_headers = headers,
                physical_line = currentPhysicalLine,
                data_row_number = dataRowNumber,
                processing_timestamp = DateTime.UtcNow
            }, new JsonSerializerOptions { WriteIndented = false });

            if (isEventMode && tempEvent != null)
            {
                tempEvent.ExtrasJson = extrasJson;
                _tempEvents.Add(tempEvent);
            }
            else
            {
                tempProduct.ExtrasJson = extrasJson;
                _tempProducts.Add(tempProduct);
            }
        }

        #endregion

        #region フロー7-9: extras_jsonからデータ取得 → 属性生成 → cl_product_attr保存

        /// <summary>
        /// フロー7〜9: データ（cl_product_attr / temp_product_event）への変換保存
        /// TempProductParsed の extras_json を元に属性(ClProductAttr)を生成し、DBに保存する
        ///
        /// 処理の流れ：
        /// 取込済みの TempProductParsed（CSV1行ごとの中間結果）を順番に処理
        /// 各行の extras_json を AttributeProcessor に渡し、EAV形式の属性へ変換
        /// 生成された ClProductAttr（属性レコード）をまとめてDBに保存
        ///   - targetEntity = "PRODUCT" → cl_product_attr へ保存
        /// </summary>
        private async Task GenerateProductAttributesAsync(
            string batchId, string groupCompanyCd, string targetEntity)
        {
            try
            {
                if (targetEntity == "PRODUCT")
                {
                    // ▼ PRODUCT 処理: cl_product_attr テーブルへ保存
                    await ProcessProductDataAsync(batchId, groupCompanyCd, targetEntity);
                }
                else if (targetEntity == "EVENT")
                {
                    // ▼ EVENT 処理: temp_product_event に保存済み（属性生成はスキップ）
                    await ProcessEventDataAsync(batchId, groupCompanyCd, targetEntity);
                }
                else
                {
                    throw new IngestException(
                        ErrorCodes.INVALID_INPUT,
                        $"サポートされていない targetEntity です: {targetEntity}",
                        recordRef: $"batch_id:{batchId}"
                    );
                }
            }
            catch (IngestException)
            {
                // 既にIngestExceptionの場合は再スロー
                throw;
            }
            catch (Exception ex)
            {
                // 想定外のエラーは DB_ERROR として再スロー
                throw new IngestException(
                    ErrorCodes.DB_ERROR,
                    $"属性生成中にエラーが発生しました: {ex.Message}",
                    ex,
                    recordRef: $"batch_id:{batchId}"
                );
            }
        }


        /// <summary>
        /// PRODUCT データ処理: cl_product_attr テーブルへの保存
        /// </summary>
        private async Task ProcessProductDataAsync(string batchId, string groupCompanyCd, string dataKind)
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

        /// <summary>
        /// EVENT データ処理: 読込段階で temp_product_event に保存済みのため属性生成はスキップ
        ///
        /// TODO: EVENT 処理の実装
        /// 必須項目:
        ///   - source_group_company_cd
        ///   - source_product_cd
        ///   - event_kind_raw
        ///   - qty_raw (非数値/ゼロ未満は行エラー)
        ///   - event_ts_raw
        ///
        /// 保存先: temp_product_event テーブル
        /// キー: temp_row_event_id (UUID採番)
        /// 冪等性: idem_key (batch_id:line_no) で重複チェック
        /// </summary>
        private async Task ProcessEventDataAsync(string batchId, string groupCompanyCd, string dataKind)
        {
            // EVENT は読み込み段階で temp_product_event に保存済みのため、ここでは何もしない
            Console.WriteLine($"EVENT属性生成スキップ: batch_id={batchId}, 件数={_tempEvents.Count}");
            await Task.CompletedTask;
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
            // 将当前 CSV 行拼成原始片段（保留整行，而不是前5个）
            string rawFragment = ex.RawFragment ?? "";
                if (string.IsNullOrEmpty(rawFragment))
                {
                    if (record != null)
                    {
                        // 使用原始 CSV 的分隔符拼回去（保持原始列顺序）
                        rawFragment = string.Join(",", record);
                    }
                    else
                    {
                        rawFragment = "";
                    }
                }
            var error = new RecordError
            {
                BatchId = batchId,// どのバッチの処理か
                Step = "INGEST",// 現在の処理段階（取込ステップ）
                RecordRef = !string.IsNullOrEmpty(ex.RecordRef) ? ex.RecordRef : $"line:{dataRowNumber}",
                ErrorCd = ex.ErrorCode,// エラーコード（例: MAPPING_NOT_FOUND）
                ErrorDetail = $"データ行 {dataRowNumber} (物理行 {currentPhysicalLine}): {ex.Message}",
                RawFragment = rawFragment// 行データの一部を保存
            };

            Console.WriteLine($"エラーレコード: [{error.ErrorCd}] {error.ErrorDetail}");
            _recordErrors.Add(error);
        }

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
                if (_tempEvents.Count > 0)
                {
                    // ▼ EVENTデータは temp_product_event へ保存（商品tempには保存しない）
                    await _productRepository.SaveTempProductEventsAsync(_tempEvents);
                    await _productRepository.SaveRecordErrorsAsync(_recordErrors);
                    Console.WriteLine($"temp保存完了: イベント={_tempEvents.Count}, エラー={_recordErrors.Count}");
                }
                else
                {
                    // ▼ 商品データ（TempProductParsed）をtempテーブルへ保存
                    await _productRepository.SaveTempProductsAsync(_tempProducts);
                    // ▼ 行エラー情報（RecordError）も同時に保存
                    await _productRepository.SaveRecordErrorsAsync(_recordErrors);
                    // ▼ 保存件数をログに出力
                    Console.WriteLine($"temp保存完了: 商品={_tempProducts.Count}, エラー={_recordErrors.Count}");
                }
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
        /// ファイルの実際の文字コードと設定された文字コードが一致するか検証する
        ///
        /// 処理内容:
        /// 1. ファイルの先頭部分を読み込んで実際のエンコーディングを検出
        /// 2. 設定されたエンコーディングで読み込んでエラーが発生しないか確認
        /// 3. 文字化けや読み込みエラーが発生した場合、INVALID_ENCODING エラーを発生
        ///
        /// この検証により、CSV読み込み処理中に文字コードの不一致によるエラーを未然に防ぐ。
        /// エンコーディング不一致はバッチ処理全体を FAILED にして即座に終了する。
        /// </summary>
        /// <param name="filePath">検証対象のファイルパス</param>
        /// <param name="expectedEncoding">設定で指定された文字コード</param>
        private void ValidateFileEncoding(string filePath, Encoding expectedEncoding)
        {
            try
            {
                Console.WriteLine($"\n--- 文字コード検証開始 ---");
                Console.WriteLine($"設定エンコーディング: {expectedEncoding.EncodingName} ({expectedEncoding.WebName})");

                // ファイルの先頭 4KB を読み込んで検証
                const int sampleSize = 4096;
                byte[] sampleBytes;

                using (var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    int bytesToRead = (int)Math.Min(sampleSize, fs.Length);
                    sampleBytes = new byte[bytesToRead];
                    fs.Read(sampleBytes, 0, bytesToRead);
                }

                // BOM (Byte Order Mark) チェック
                string detectedEncodingName = DetectEncodingFromBOM(sampleBytes);
                if (!string.IsNullOrEmpty(detectedEncodingName))
                {
                    Console.WriteLine($"検出されたBOM: {detectedEncodingName}");

                    // BOM が検出された場合、設定と一致するか確認
                    if (!IsEncodingCompatible(detectedEncodingName, expectedEncoding))
                    {
                        throw new IngestException(
                            ErrorCodes.INVALID_ENCODING,
                            $"文字コード不一致: ファイルのBOMは {detectedEncodingName} ですが、設定は {expectedEncoding.WebName} です。" +
                            $"バッチ処理を終了します。",
                            recordRef: $"file:{filePath}"
                        );
                    }
                }

                // 設定されたエンコーディングで実際に読み込んでみて、エラーが発生しないか確認
                try
                {
                    string testContent = expectedEncoding.GetString(sampleBytes);

                    // 文字化けの可能性をチェック（置換文字 � (U+FFFD) が含まれているか）
                    if (testContent.Contains('\uFFFD'))
                    {
                        throw new IngestException(
                            ErrorCodes.INVALID_ENCODING,
                            $"文字コード不一致: {expectedEncoding.WebName} で読み込むと文字化けが発生します。" +
                            $"ファイルの実際のエンコーディングを確認してください。バッチ処理を終了します。",
                            recordRef: $"file:{filePath}"
                        );
                    }

                    Console.WriteLine($"✓ 文字コード検証成功: {expectedEncoding.WebName}");
                }
                catch (DecoderFallbackException dex)
                {
                    throw new IngestException(
                        ErrorCodes.INVALID_ENCODING,
                        $"文字コード不一致: {expectedEncoding.WebName} でデコードできません。" +
                        $"ファイルの実際のエンコーディングを確認してください。バッチ処理を終了します。",
                        dex,
                        recordRef: $"file:{filePath}"
                    );
                }
            }
            catch (IngestException)
            {
                // IngestException はそのまま再スロー
                throw;
            }
            catch (Exception ex)
            {
                throw new IngestException(
                    ErrorCodes.INVALID_ENCODING,
                    $"文字コード検証中にエラーが発生しました: {ex.Message}",
                    ex,
                    recordRef: $"file:{filePath}"
                );
            }
        }

        /// <summary>
        /// BOM (Byte Order Mark) から文字コードを検出する
        /// </summary>
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

            return string.Empty; // BOM なし
        }

        /// <summary>
        /// 検出されたエンコーディング名と設定されたエンコーディングが互換性があるか確認
        /// </summary>
        private bool IsEncodingCompatible(string detectedName, Encoding expectedEncoding)
        {
            string expectedName = expectedEncoding.WebName.ToUpperInvariant();
            string detected = detectedName.ToUpperInvariant();

            // UTF-8 の場合
            if (detected.Contains("UTF-8") || detected.Contains("UTF8"))
                return expectedName.Contains("UTF-8") || expectedName.Contains("UTF8");

            // UTF-16 の場合
            if (detected.Contains("UTF-16"))
                return expectedName.Contains("UTF-16") || expectedName.Contains("UNICODE");

            // その他
            return detected == expectedName;
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

        /// <summary>
        /// データベースのNOT NULL列情報を取得してキャッシュする
        /// </summary>
        private async Task LoadNotNullColumnsAsync()
        {
            if (_notNullColumnsCache != null)
                return; // 既にキャッシュ済み

            _notNullColumnsCache = new Dictionary<string, HashSet<string>>();

            using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync();

            // PostgreSQLのinformation_schemaから列情報を取得
            var sql = @"
                SELECT
                    table_name,
                    column_name,
                    is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name IN ('temp_product_parsed', 'temp_product_event', 'cl_product_attr')
                ORDER BY table_name, ordinal_position";

            var columns = await connection.QueryAsync<(string TableName, string ColumnName, string IsNullable)>(sql);

            foreach (var (tableName, columnName, isNullable) in columns)
            {
                if (!_notNullColumnsCache.ContainsKey(tableName))
                {
                    _notNullColumnsCache[tableName] = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                }

                // is_nullable = 'NO' の場合、NOT NULL制約がある
                if (isNullable == "NO")
                {
                    _notNullColumnsCache[tableName].Add(columnName);
                }
            }

            Console.WriteLine($"データベーススキーマ情報をキャッシュしました:");
            foreach (var (tableName, notNullCols) in _notNullColumnsCache)
            {
                Console.WriteLine($"  {tableName}: {notNullCols.Count}個のNOT NULL列");
            }
        }

        /// <summary>
        /// 指定されたテーブルの列がNOT NULL制約を持つかチェック
        /// </summary>
        private bool IsColumnNotNull(string tableName, string columnName)
        {
            if (_notNullColumnsCache == null)
                return false;

            if (!_notNullColumnsCache.TryGetValue(tableName, out var notNullColumns))
                return false;

            return notNullColumns.Contains(columnName);
        }

        #endregion
    }
}
