using ProductDataIngestion.Services; // サービス層（業務ロジック）を利用するため
using ProductDataIngestion.Repositories; // リポジトリ層（DB操作）を利用するため
using ProductDataIngestion.Models; // エラーコード定数を利用するため
using Microsoft.Extensions.Configuration;  // 設定ファイル（JSONなど）を読み込むためのライブラリ

/// <summary>
/// 商品データ取込システムのメインプログラム
/// CSVファイルから商品データを読み取り、データベースに取り込む処理を行う
/// </summary>
class Program
{
    /// <summary>
    /// プログラムのエントリーポイント
    /// 1. 設定ファイルの読み込み
    /// 2. CSVファイルの存在確認
    /// 3. データベースの接続確認
    /// 4. CSVデータ取込処理を実行
    /// </summary>
    static async Task Main(string[] args)
    {
        // コンソール出力をUTF-8に設定
        Console.OutputEncoding = System.Text.Encoding.UTF8;
        Console.WriteLine("< 商品データ取込システム >");

        try
        {
            // 設定ファイル (appsettings.json) と環境変数を読み込む
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())  
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            // 必須設定を取得する（未設定ならエラー）
            // CsvImport:DefaultFilePath → 取込対象CSVファイルのパス
            // DefaultConnection → データベースの接続文字列
            string csvFilePath = configuration.GetValue<string>("CsvImport:DefaultFilePath") 
                ?? throw new ArgumentException("エラー: CSVファイルパスが未設定です (appsettings.jsonを確認)");
            string connectionString = configuration.GetConnectionString("DefaultConnection") 
                ?? throw new ArgumentException("エラー: データベース接続情報が未設定です (appsettings.jsonを確認)");
            
            // 取込対象の設定（固定値）
            string groupCompanyCd = "KM";// 会社コード（固定）
            string targetEntity = "EVENT";// 対象データ（商品）

            //// CSVファイルが存在するかをチェック
            if (!File.Exists(csvFilePath))
            {
                Console.WriteLine($"エラー: ファイルが見つかりません: {csvFilePath}");
                return;// ファイルがない場合は終了  
            }

            // ▼ データ取込設定（マッピング情報など）を取得し、部分内容を表示する
            try
            {
                // DBから設定を取得するためのリポジトリとサービスを作成
                var dataRepository = new DataImportRepository(connectionString);
                var dataService = new DataImportService(dataRepository);
                // 設定を検索するためのキーを作成
                string usageNm = $"{groupCompanyCd}-PRODUCT";
                Console.WriteLine($"\n--- 取込設定の読み込み (用途: {usageNm}) ---");
                // 設定情報（プロファイル）を取得
                var setting = await dataService.GetImportSettingAsync(groupCompanyCd, usageNm);
                // 設定の概要を表示
                Console.WriteLine($"ProfileId: {setting.ProfileId}, 区切り文字: '{setting.Delimiter}', ヘッダー行: {setting.HeaderRowIndex}");

                // 列マッピング設定の最初の10件だけ表示して内容を確認する
                Console.WriteLine($"\n--- 列マッピング設定（最初の10件） ---");
                var details = await dataService.GetImportDetailsAsync(setting.ProfileId);
                int take = Math.Min(10, details.Count);
                if (take == 0)
                {
                    Console.WriteLine("(マッピング設定が見つかりません)");
                }
                else
                {
                    // 各列の設定を出力
                    for (int i = 0; i < take; i++)
                    {
                        var d = details[i];
                        Console.WriteLine($"{i + 1}. ColumnSeq={d.ColumnSeq}, ProjectionKind={d.ProjectionKind}, TargetColumn={d.TargetColumn}, AttrCd={d.AttrCd}, IsRequired={d.IsRequired}, TransformExpr={d.TransformExpr}");
                    }
                    // 10件以上ある場合に残り件数を表示
                    if (details.Count > take)
                        Console.WriteLine($"... 他に {details.Count - take} 件の設定があります");
                }
            }
            catch (Exception ex)
            {
                // 設定の読み込みでエラーがあっても処理を続行する
                Console.WriteLine($"設定読み込みエラー: {ex.Message}");
                Console.WriteLine("取込処理を続行します\n");
            }

             // ▼ 各種リポジトリ（DB操作クラス）を初期化
            var batchRepository = new BatchRepository(connectionString); // バッチ情報を扱う
            var productRepository = new ProductRepository(connectionString); // 商品データを扱う

            // ▼ CSV取込サービスを作成
            // IngestService は CSVの読み込み・変換・DB保存などを担当
            var ingestService = new IngestService(connectionString, batchRepository, productRepository);
            //CSVを処理し、結果のバッチIDを受け取る
            string batchId = await ingestService.ProcessCsvFileAsync(csvFilePath, groupCompanyCd, targetEntity);
            
            // ▼ 実行結果を出力
            Console.WriteLine($"\nバッチID: {batchId}");
            Console.WriteLine("\n処理が完了しました。");
        }
        catch (Npgsql.NpgsqlException ex)
        {
            // データベース接続エラーの場合は専用のメッセージを表示
            Console.WriteLine($"\nデータベース接続エラーが発生しました:");
            Console.WriteLine($"エラーコード: {ex.ErrorCode}");
            Console.WriteLine($"メッセージ: {ex.Message}");
            Console.WriteLine("接続文字列とデータベースの状態を確認してください。");
            Console.WriteLine($"\nスタックトレース:\n{ex.StackTrace}");
            
            // 設計書で定義されたDB_ERRORコードを使用してIngestExceptionでラップする
            throw new IngestException(
                ErrorCodes.DB_ERROR, 
                "データベースに接続できません。設定を確認してください。", 
                ex,  // innerException
                $"ErrorCode: {ex.ErrorCode}" // rawFragment
            );
        }
        catch (Exception ex)
        {
            // その他の予期しないエラーの場合
            Console.WriteLine($"\n予期しないエラーが発生しました:");
            Console.WriteLine($"メッセージ: {ex.Message}");
            Console.WriteLine($"スタックトレース:\n{ex.StackTrace}");
            
            // 予期しないエラーの場合もDB_ERRORでラップする（既にIngestExceptionの場合を除く）
            if (ex is not IngestException)
            {
                throw new IngestException(
                    ErrorCodes.DB_ERROR,
                    $"予期しないデータベースエラーが発生しました: {ex.Message}",
                    ex // innerException
                );
            }
            throw;
        }
        Console.WriteLine("\nEnterキーを押して終了してください...");
        Console.ReadLine();
    }
}