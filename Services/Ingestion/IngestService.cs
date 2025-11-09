using CsvHelper.Configuration;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using ProductDataIngestion.Models;
using ProductDataIngestion.Repositories;
using ProductDataIngestion.Repositories.Company;
using ProductDataIngestion.Repositories.Interfaces;
using ProductDataIngestion.Services.Ingestion;

namespace ProductDataIngestion.Services
{
    /// <summary>
    /// CSV取込フローを統括するサービス。
    /// 詳細なCSV解析やバッチ管理は専用サービスへ委譲し、ここでは全体制御のみを担う。
    /// 
    /// 役割：
    /// - ファイルパスを受け取り、DBにデータを安全に登録する。
    /// - CSV解析、ルール取得、temp保存、属性展開、バッチ完了報告までを一貫制御。
    /// 
    /// 範囲：
    /// - 詳細な業務ロジックは専用サービスへ委譲（単一責任原則を維持）
    ///   例：CSV解析 → CsvTempIngestionService、バッチ履歴 → IngestionBatchService
    /// 
    /// 主な呼び出し元：
    /// - Program.cs（コンソール起動点）
    /// </summary>
    public class IngestService
    {
        // ★ フィールド定義
        private readonly DataImportService _dataService;// m_data_import_setting / d / map 関連のルール取得を担当
        private readonly IProductRepository _productRepository;// temp_product / cl_product_attr / record_error へのDB操作担当
        private readonly IDataImportRepository _dataRepository;// DataImportService 内で利用するDBアクセス層
        private readonly CsvValidator _csvValidator; // CSVデータの形式・必須項目などを検証するユーティリティ
        private readonly AttributeProcessor _attributeProcessor;// extras_json → 属性（cl_product_attr）への展開担当
        private readonly string _connectionString; // PostgreSQL 接続文字列
        private readonly DatabaseSchemaInspector _schemaInspector;// DBのテーブル構造・存在チェック（開発/検証環境対応）
        private readonly CsvTempIngestionService _csvTempService; // CSV読込～temp保存の一連処理担当
        private readonly IngestionBatchService _batchService; // batch_run テーブルの起票・更新
        private readonly CompanyValidator _companyValidator; // GP会社コード（KM/RKE/KBOなど）の妥当性検証担当

        /// <summary>
        /// コンストラクタ：IngestServiceの初期化処理。
        /// 
        /// 【目的】
        /// - 各サービス・リポジトリを生成し、依存関係を明示的に組み立てる。
        /// - Nullチェックを厳密に行い、安全な初期状態を保証。
        /// </summary>
        public IngestService(
            string connectionString,
            IBatchRepository batchRepository,
            IProductRepository productRepository)
        {
            // ★ DB接続文字列がnullの場合は即エラー（環境設定ミス防止）
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            // ★ Repositoryが外部から正しく注入されているかを確認
            if (batchRepository == null) throw new ArgumentNullException(nameof(batchRepository));
            _productRepository = productRepository ?? throw new ArgumentNullException(nameof(productRepository));
            // ★ 各種サービスを初期化
            _dataRepository = new DataImportRepository(_connectionString);// インポート設定取得用
            _dataService = new DataImportService(_dataRepository); // 上記を利用するサービス層
            _csvValidator = new CsvValidator();// CSVフォーマット検証
            _attributeProcessor = new AttributeProcessor(_dataService);// 属性展開
                                                                       // ★ DB構造を確認するサービス（init.sql が反映されているかなど）
            _schemaInspector = new DatabaseSchemaInspector(_connectionString);
            // ★ CSV取込時の一時保存処理を行うサービス
            _csvTempService = new CsvTempIngestionService(_csvValidator, _schemaInspector);
            // ★ バッチ履歴（開始・終了・統計更新など）を管理するサービス
            _batchService = new IngestionBatchService(batchRepository);
            // ★ GP会社コードの検証用サービス（m_company参照 or Fallback）
            var companyRepository = new CompanyRepository(_connectionString);
            _companyValidator = new CompanyValidator(companyRepository);
        }

        /// <summary>
        /// CSVファイルを処理して、取込バッチを実行するメイン関数。
        /// 
        /// 【処理フロー】
        /// 1.スキーマ確認（DB構造が存在するか）
        /// 2.会社コード検証（KM/RKE/KBOなど）
        /// 3.バッチ起票（batch_runに登録）
        /// 4.取込設定・マッピングルール取得
        /// 5.CSV読込 → tempテーブル保存
        /// 6.属性展開（cl_product_attrへ）
        /// 7.統計更新 → 完了ログ出力
        /// </summary>
        public async Task<string> ProcessCsvFileAsync(string filePath, string groupCompanyCd, string targetEntity)
        {
            Console.WriteLine($"=== CSV取込開始 ===\nファイル: {filePath}\nGPコード: {groupCompanyCd}\n対象エンティティ: {targetEntity}");
            // Step 1: DBスキーマをロード（存在確認）
            await _schemaInspector.EnsureLoadedAsync();
            // Step 2: GP会社コード検証
            // m_company テーブル or 定義済みFallbackリストで有効性をチェックする。
            await _companyValidator.ValidateAsync(groupCompanyCd);
            // Step 3: バッチ起票
            // batch_runテーブルに新しいバッチを登録し、バッチIDを生成。
            var batchId = await _batchService.CreateBatchAsync(filePath, groupCompanyCd, targetEntity);
            // Step 4: 作業データ格納用リスト初期化
            var tempProducts = new List<TempProductParsed>();// 商品データ行
            var tempEvents = new List<TempProductEvent>();// イベントデータ行
            var recordErrors = new List<RecordError>();// エラー行

            try
            {
                // Step 5: 取込設定・マッピングルール取得
                var (importSetting, importDetails) = await FetchImportRulesAsync(groupCompanyCd, targetEntity);
                // Step 6: CSV設定構築（区切り文字・エンコーディング）
                var (config, headerRowIndex) = ConfigureCsvReaderSettings(importSetting);
                // Step 7: CSV読込 → tempテーブル保存
                // ここで実際にファイルを1行ずつ解析・検証し、temp_product_parsed / event に保存。
                var result = await _csvTempService.ReadCsvAndSaveToTempAsync(
                    filePath,
                    batchId,
                    groupCompanyCd,
                    headerRowIndex,
                    targetEntity, 
                    importDetails,
                    config,
                    tempProducts,
                    tempEvents,
                    recordErrors);
                // Step 8: tempテーブル保存（PRODUCT/EVENT分岐）
                if (string.Equals(targetEntity, "EVENT", StringComparison.OrdinalIgnoreCase))
                {
                    // EVENTデータ（在庫・販売履歴など）を保存
                    await _productRepository.SaveTempProductEventsAsync(tempEvents);
                }
                else
                {
                    // PRODUCTデータ（商品マスタ関連）を保存
                    await _productRepository.SaveTempProductsAsync(tempProducts);
                }
                // Step 9: エラーレコードを保存
                await _productRepository.SaveRecordErrorsAsync(recordErrors);// CsvValidatorで検出されたエラーをrecord_errorテーブルへ登録。
                // Step 10: 属性データ生成・保存
                await GenerateProductAttributesAsync(batchId, groupCompanyCd, targetEntity, tempProducts, tempEvents);// extras_json などを元に cl_product_attr へ展開保存。
                                                                                                                      // Step 11: 統計更新
                await _batchService.UpdateStatisticsAsync(batchId, result);// 読込件数・成功件数・失敗件数などを batch_run に反映。
                // Step 12: 完了ログ出力
                Console.WriteLine($"=== 取込完了 ===\n読込: {result.readCount}\n成功: {result.okCount}\n失敗: {result.ngCount}");
                return batchId;
            }
            catch (IngestException ex)
            {
                // ★ 業務例外発生時の処理：
                // 例：必須列なし、マッピング定義なし、型変換エラーなど。
                // これらは想定内のエラーとして record_error に登録。
                await RecordErrorAndMarkBatchFailed(batchId, ex, recordErrors);
                throw;// 上位へ再スローして Program.cs 側でログ出力
            }
            catch (Exception ex)
            {
                // ★ 想定外（SystemExceptionなど）：
                // 例：DB接続断、NullReferenceException、I/O失敗など。
                var dbError = new IngestException(
                    ErrorCodes.DB_ERROR,
                    $"想定外のエラーが発生しました: {ex.Message}",
                    ex
                );
                await RecordErrorAndMarkBatchFailed(batchId, dbError, recordErrors);
                throw dbError;
            }
        }

        // ▼ 以下、補助的な内部処理（設定取得・属性展開・エラー処理など）
        // 取込設定（m_data_import_setting / d）を取得し、複数存在する場合はエラー。
        private async Task<(MDataImportSetting, List<MDataImportD>)> FetchImportRulesAsync(string groupCompanyCd, string targetEntity)
        {
            try
            {
                // 指定会社・対象種別で設定を検索
                var candidates = await _dataService.GetActiveImportSettingsAsync(groupCompanyCd, targetEntity);
                //見つからなかった場合はエラー
                if (candidates == null || candidates.Count == 0)
                {
                    throw new IngestException(
                        ErrorCodes.MAPPING_NOT_FOUND,
                        $"取込設定が未登録です: group_company_cd={groupCompanyCd}, target_entity={targetEntity}"
                    );
                }
                //複数を存在したら　もエラー
                if (candidates.Count > 1)
                {
                    var ids = string.Join(",", candidates.Select(c => c.ProfileId));
                    throw new IngestException(
                        ErrorCodes.MAPPING_NOT_FOUND,
                        $"有効な取込設定が複数存在します。設定を特定してください: profiles=[{ids}]"
                    );
                }
                // 設定1件を確定し、詳細列定義を取得
                var importSetting = candidates[0];
                var importDetails = await _dataService.GetImportDetailsAsync(importSetting.ProfileId);
                Console.WriteLine($"取込マッピング: profile_id={importSetting.ProfileId}, 明細={importDetails.Count}");

                return (importSetting, importDetails);
            }
            catch (IngestException)
            {
                throw;
            }
            catch (Exception ex)
            {
                // DB接続やSQL実行エラー
                throw new IngestException(
                    ErrorCodes.DB_ERROR,
                    $"取込設定の取得中にエラーが発生しました: {ex.Message}",
                    ex
                );
            }
        }
        // CSV読込設定（区切り文字・ヘッダ行位置・文字コード）を構築
        private (CsvConfiguration, int) ConfigureCsvReaderSettings(MDataImportSetting importSetting)
        {
            try
            {
                var config = new CsvConfiguration(CultureInfo.InvariantCulture)
                {
                    HasHeaderRecord = importSetting.HeaderRowIndex > 0,// ヘッダー行が存在するか
                    Delimiter = importSetting.Delimiter ?? ",",// 区切り文字（デフォルトはカンマ）
                    BadDataFound = _ => { },// 不正行を無視
                    MissingFieldFound = null,// 欠落列を無視
                    Encoding = GetEncodingFromCharacterCode(importSetting.CharacterCd ?? "UTF-8")
                };

                return (config, importSetting.HeaderRowIndex);
            }
            catch (Exception ex)
            {
                // 文字コード指定ミスなど
                throw new IngestException(
                    ErrorCodes.INVALID_ENCODING,
                    $"CSV設定の構築に失敗しました: {ex.Message}",
                    ex
                );
            }
        }
        // 商品・イベントデータに応じて属性生成処理を分岐
        private async Task GenerateProductAttributesAsync(
            string batchId,
            string groupCompanyCd,
            string targetEntity,
            IReadOnlyList<TempProductParsed> tempProducts,
            IReadOnlyList<TempProductEvent> tempEvents)
        {
            try
            {
                if (string.Equals(targetEntity, "PRODUCT", StringComparison.OrdinalIgnoreCase))
                {
                    // 商品属性展開
                    await ProcessProductDataAsync(batchId, groupCompanyCd, targetEntity, tempProducts);
                }
                else if (string.Equals(targetEntity, "EVENT", StringComparison.OrdinalIgnoreCase))
                {
                    // イベント属性展開（今後拡張予定）
                    await ProcessEventDataAsync(batchId, groupCompanyCd, targetEntity, tempEvents);
                }
                else
                {
                    throw new IngestException(
                        ErrorCodes.INVALID_INPUT,
                        $"未対応の targetEntity です: {targetEntity}",
                        recordRef: $"batch_id:{batchId}"
                    );
                }
            }
            catch (IngestException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new IngestException(
                    ErrorCodes.DB_ERROR,
                    $"派生データ生成でエラーが発生しました: {ex.Message}",
                    ex,
                    recordRef: $"batch_id:{batchId}"
                );
            }
        }
        // temp_product_parsed → cl_product_attr 変換処理
        private async Task ProcessProductDataAsync(
            string batchId,
            string groupCompanyCd,
            string dataKind,
            IReadOnlyList<TempProductParsed> tempProducts)
        {
            if (tempProducts.Count == 0)
            {
                Console.WriteLine("PRODUCT取込対象が存在しないため、cl_product_attr への保存をスキップします。");
                return;
            }

            var allProductAttrs = new List<ClProductAttr>();

            foreach (var tempProduct in tempProducts)
            {
                // extras_json を解析して属性リストを生成
                var attrs = await _attributeProcessor.ProcessAttributesAsync(
                    batchId,
                    tempProduct,
                    groupCompanyCd,
                    dataKind
                );
                allProductAttrs.AddRange(attrs);
            }
            // 一括保存
            await _productRepository.SaveProductAttributesAsync(allProductAttrs);
            Console.WriteLine($"cl_product_attr へ {allProductAttrs.Count} 件の属性データを保存しました。");
        }
        // EVENTデータ（在庫・販売）の処理（未実装ダミー）
        private async Task ProcessEventDataAsync(
            string batchId,
            string groupCompanyCd,
            string dataKind,
            IReadOnlyList<TempProductEvent> tempEvents)
        {
            Console.WriteLine($"EVENT取込ステップ: batch_id={batchId}, 件数={tempEvents.Count}");
            await Task.CompletedTask;
        }
        // エラー発生時：record_errorへ登録し、バッチをFAILEDに更新
        private async Task RecordErrorAndMarkBatchFailed(
            string batchId,
            IngestException ex,
            List<RecordError> recordErrors)
        {
            var error = new RecordError
            {
                BatchId = batchId,
                Step = "INGEST",
                RecordRef = ex.RecordRef ?? string.Empty,
                ErrorCd = ex.ErrorCode,
                ErrorDetail = ex.Message,
                RawFragment = ex.RawFragment ?? string.Empty
            };

            recordErrors.Add(error);
            // record_error へ保存
            await _productRepository.SaveRecordErrorsAsync(recordErrors);
            // batch_run ステータスを FAILED に変更
            await _batchService.MarkFailedAsync(batchId, ex.Message);
        }
        // CSV文字コードを判定してEncodingを返す（SHIFT_JIS / UTF-8 / EUC-JP）
        private Encoding GetEncodingFromCharacterCode(string characterCd)
        {
            return characterCd?.ToUpperInvariant() switch
            {
                "UTF-8" => Encoding.UTF8,
                "SHIFT_JIS" => Encoding.GetEncoding("Shift_JIS"),
                "EUC-JP" => Encoding.GetEncoding("EUC-JP"),
                _ => Encoding.UTF8// 不明指定時はUTF-8で処理
            };
        }
    }
}
