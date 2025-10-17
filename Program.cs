using ProductDataIngestion.Services;

class Program
{
    static async Task Main(string[] args)
    {
        Console.OutputEncoding = System.Text.Encoding.UTF8;
        Console.WriteLine("========================================");
        Console.WriteLine("  商品データ取込システム");
        Console.WriteLine("========================================\n");

        try
        {
            // CSV文件路径(请修改为您的实际文件路径)
            string csvFilePath = @"C:\Users\yang.jie.tw\Documents\ProductDataIngestion\KM商品データ追加ROLEX.csv";

            // 检查文件是否存在
            if (!File.Exists(csvFilePath))
            {
                Console.WriteLine($"エラー: ファイルが見つかりません: {csvFilePath}");
                Console.WriteLine("\nサンプルCSVファイルを作成してください:");
                Console.WriteLine("sample_products.csv に以下の内容:");
                Console.WriteLine("GP会社コード,商品コード,ブランドID,ブランド名,カテゴリ1ID");
                Console.WriteLine("KM,0000123456,4952,ROLEX,20");
                Console.WriteLine("KM,0000123457,4953,OMEGA,20");
                return;
            }

            // 数据库连接字符串 - 根据您的docker-compose配置
            string connectionString = "Host=localhost;Port=25432;Database=purchase_system;Username=postgres;Password=user";

            // GP会社コード
            string groupCompanyCd = "KM";

            // 在执行导入之前，先打印 m_data_import_d 的前几条映射以便排查
            try
            {
                var dataService = new ProductDataIngestion.Services.DataImportService(connectionString);
                string usageNm = $"{groupCompanyCd}-PRODUCT";
                Console.WriteLine($"\n--- 尝试读取 ImportSetting (usage: {usageNm}) ---");
                var setting = await dataService.GetImportSettingAsync(groupCompanyCd, usageNm);
                Console.WriteLine($"ProfileId: {setting.ProfileId}, Delimiter: '{setting.Delimiter}', HeaderRowIndex: {setting.HeaderRowIndex}");

                Console.WriteLine($"\n--- 读取 m_data_import_d 的前 20 条映射 ---");
                var details = await dataService.GetImportDetailsAsync(setting.ProfileId);
                int take = Math.Min(20, details.Count);
                if (take == 0)
                {
                    Console.WriteLine("(未找到任何映射)");
                }
                else
                {
                    for (int i = 0; i < take; i++)
                    {
                        var d = details[i];
                        Console.WriteLine($"{i + 1}. ColumnSeq={d.ColumnSeq}, TargetEntity={d.TargetEntity}, TargetColumn={d.TargetColumn}, AttrCd={d.AttrCd}, IsRequired={d.IsRequired}, TransformExpr={d.TransformExpr}");
                    }
                    if (details.Count > take)
                        Console.WriteLine($"... 还有 {details.Count - take} 条映射未显示");
                }

                Console.WriteLine("\n已打印映射，程序将退出以避免继续执行导入。如需继续导入，请再次运行程序或移除此打印逻辑。\n");
                return;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"读取映射时出错: {ex.Message}");
                Console.WriteLine("将继续执行常规流程（导入）\n");
            }

            // 取込サービスのインスタンス作成 - 传入连接字符串
            var ingestService = new IngestService(connectionString);

            // CSV取込処理実行
            string batchId = await ingestService.ProcessCsvFileAsync(csvFilePath, groupCompanyCd);

            // 結果表示
            ingestService.PrintResults();

            Console.WriteLine($"\nバッチID: {batchId}");
            Console.WriteLine("\n処理が完了しました。");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n予期しないエラーが発生しました:");
            Console.WriteLine($"メッセージ: {ex.Message}");
            Console.WriteLine($"スタックトレース:\n{ex.StackTrace}");
        }

        Console.WriteLine("\nEnterキーを押して終了してください...");
        Console.ReadLine();
    }
}