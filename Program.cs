using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using ProductDataIngestion.Models;
using ProductDataIngestion.Repositories;
using ProductDataIngestion.Repositories.Company;
using ProductDataIngestion.Repositories.Interfaces;
using ProductDataIngestion.Services;
using ProductDataIngestion.Services.Upsert;
using ProductDataIngestion.Utils;

class Program
{
    static async Task Main(string[] args)
    {
        Dapper.DefaultTypeMap.MatchNamesWithUnderscores = true;
        Console.OutputEncoding = Encoding.UTF8;
        Console.WriteLine("< 商品データ取込システム >");

        string? batchId = null;
        UpsertService? upsertService = null;
        IServiceScope? serviceScope = null;

        try
        {
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            string csvFilePath = configuration.GetValue<string>("CsvImport:DefaultFilePath")
                ?? throw new ArgumentException("エラー: CSVファイルパスが未設定です (appsettings.json を確認してください)。");
            string connectionString = configuration.GetConnectionString("DefaultConnection")
                ?? throw new ArgumentException("エラー: データベース接続情報が未設定です (appsettings.json を確認してください)。");

            const string groupCompanyCd = "KM";
            const string targetEntity = "PRODUCT";

            if (!File.Exists(csvFilePath))
            {
                Console.WriteLine($"エラー: ファイルが見つかりません: {csvFilePath}");
                return;
            }

            var batchRepository = new BatchRepository(connectionString);
            var productRepository = new ProductRepository(connectionString);
            var ingestService = new IngestService(connectionString, batchRepository, productRepository);

            batchId = await ingestService.ProcessCsvFileAsync(csvFilePath, groupCompanyCd, targetEntity);
            Console.WriteLine($"\nバッチID: {batchId}");

            using IHost host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                {
                    services.AddScoped<IClProductAttrRepository>(_ => new ClProductAttrRepository(connectionString));
                    services.AddScoped<IAttributeDefinitionRepository>(_ => new AttributeDefinitionRepository(connectionString));
                    services.AddScoped<ICleansePolicyRepository>(_ => new CleansePolicyRepository(connectionString));
                    services.AddScoped<IRecordErrorRepository>(_ => new RecordErrorRepository(connectionString));
                    services.AddScoped<IRefTableMapRepository>(_ => new RefTableMapRepository(connectionString));
                    services.AddScoped<IBrandSourceMapRepository>(_ => new BrandSourceMapRepository(connectionString));
                    services.AddScoped<IMBrandGRepository>(_ => new MBrandGRepository(connectionString));
                    services.AddScoped<ICompanyRepository>(_ => new CompanyRepository(connectionString));
                    services.AddScoped<IMCompanyRepository>(_ => new MCompanyRepository(connectionString));
                    services.AddScoped<IAttrSourceMapRepository>(_ => new AttrSourceMapRepository(connectionString));
                    services.AddScoped<ICategorySourceMapRepository>(_ => new CategorySourceMapRepository(connectionString));
                    services.AddScoped<IMCategoryGRepository>(_ => new MCategoryGRepository(connectionString));
                    services.AddScoped<IMListItemGRepository>(_ => new MListItemGRepository(connectionString));
                    services.AddScoped<IRefResolverRepository>(_ => new RefResolverRepository(connectionString));
                    services.AddScoped<IMCleanseRuleSetRepository>(_ => new MCleanseRuleSetRepository(connectionString));
                    services.AddScoped<IBatchRepository>(_ => new BatchRepository(connectionString));
                    services.AddScoped<IUpsertRepository>(_ => new UpsertRepository(connectionString));
                    services.AddScoped<IProductManagementRepository>(_ => new ProductManagementRepository());

                    services.AddScoped<CleansingService>();
                    services.AddScoped<UpsertService>();
                })
                .Build();

            serviceScope = host.Services.CreateScope();
            var provider = serviceScope.ServiceProvider;

            var cleansingService = provider.GetRequiredService<CleansingService>();
            var definitionRepo = provider.GetRequiredService<IAttributeDefinitionRepository>();
            var firstDefinition = (await definitionRepo.GetAllAttrDefinitionAsync()).FirstOrDefault()
                ?? new AttributeDefinition { AttrCd = "DUMMY", DataType = "TEXT" };

            Logger.Info("\n--- クレンジング処理を開始します ---");
            await cleansingService.InitializeAsync();
            await cleansingService.StartCleanseAsync(batchId);
            await cleansingService.ProcessAllAttributesAsync(batchId);
            Logger.Info("--- クレンジング処理が完了しました ---");

            upsertService = provider.GetRequiredService<UpsertService>();
            Logger.Info("\n--- UPSERT処理を開始します ---");
            await upsertService.ExecuteAsync(batchId);
            Logger.Info("--- UPSERT処理が完了しました ---");

            await host.StopAsync();

            Console.WriteLine("\n全ての処理が完了しました。");
        }
        catch (NpgsqlException ex)
        {
            Console.WriteLine("\nデータベース接続エラーが発生しました:");
            Console.WriteLine($"エラーコード: {ex.ErrorCode}");
            Console.WriteLine($"メッセージ: {ex.Message}");
            Console.WriteLine($"スタックトレース:\n{ex.StackTrace}");

            throw new IngestException(
                ErrorCodes.DB_ERROR,
                "データベースに接続できません。設定を確認してください。",
                ex,
                $"ErrorCode: {ex.ErrorCode}");
        }
        catch (Exception ex)
        {
            if (batchId != null)
            {
                try
                {
                    if (upsertService != null)
                    {
                        await upsertService.MarkBatchFailedAsync(batchId);
                    }
                    else if (serviceScope != null)
                    {
                        var fallback = serviceScope.ServiceProvider.GetRequiredService<UpsertService>();
                        await fallback.MarkBatchFailedAsync(batchId);
                    }
                }
                catch (Exception markEx)
                {
                    Console.WriteLine($"UPSERT異常終了処理で追加エラー: {markEx.Message}");
                }
            }

            Console.WriteLine("\n予期しないエラーが発生しました:");
            Console.WriteLine($"メッセージ: {ex.Message}");
            Console.WriteLine($"スタックトレース:\n{ex.StackTrace}");

            if (ex is not IngestException)
            {
                throw new IngestException(
                    ErrorCodes.DB_ERROR,
                    $"予期しないデータベースエラーが発生しました: {ex.Message}",
                    ex);
            }
            throw;
        }
        finally
        {
            serviceScope?.Dispose();
        }

        Console.WriteLine("\nEnterキーを押して終了してください...");
        Console.ReadLine();
    }
}
