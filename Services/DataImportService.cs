// Services/DataImportService.cs
// データインポートサービス：業務ロジック（CSV処理）とリポジトリを分離

using ProductDataIngestion.Models;
using ProductDataIngestion.Repositories.Interfaces;
using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;
using System.Text;
using Microsoft.Extensions.Logging;
using System.Reflection;

namespace ProductDataIngestion.Services
{
    /// データインポートサービスインターフェース：コア操作定義
    public interface IDataImportService
    {
        /// インポート設定非同期取得
        Task<MDataImportSetting> GetImportSettingAsync(string groupCompanyCd, string usageNm);

        /// インポート明細非同期取得
        Task<List<MDataImportD>> GetImportDetailsAsync(long profileId);

        /// 固定属性マッピング非同期取得
        Task<List<MFixedToAttrMap>> GetFixedToAttrMapsAsync(string groupCompanyCd, string dataKind);

        /// 属性定義非同期取得
        Task<List<MAttrDefinition>> GetAttrDefinitionsAsync();

        /// CSVを型付きレコードリストとして非同期読み込み
        Task<List<T>> ReadCsvWithSettingsAsync<T>(string filePath, MDataImportSetting setting) where T : class, new();

        /// CSVを生文字列配列リストとして非同期読み込み
        Task<List<string[]>> ReadCsvRawAsync(string filePath, MDataImportSetting setting);
    }

    /// データインポートサービス実装：リポジトリ依存でDBアクセス委譲
    public class DataImportService : IDataImportService
    {
        private readonly IDataImportRepository _repository;
        private readonly ILogger<DataImportService>? _logger;

        /// コンストラクタ：リポジトリとロガーを注入
        public DataImportService(IDataImportRepository repository, ILogger<DataImportService>? logger = null)
        {
            _repository = repository ?? throw new ArgumentNullException(nameof(repository));
            _logger = logger;
        }

        /// リポジトリ経由で設定取得
        public async Task<MDataImportSetting> GetImportSettingAsync(string groupCompanyCd, string usageNm)
        {
            return await _repository.GetImportSettingAsync(groupCompanyCd, usageNm);
        }

        /// リポジトリ経由で明細取得
        public async Task<List<MDataImportD>> GetImportDetailsAsync(long profileId)
        {
            return await _repository.GetImportDetailsAsync(profileId);
        }

        /// リポジトリ経由でマッピング取得
        public async Task<List<MFixedToAttrMap>> GetFixedToAttrMapsAsync(string groupCompanyCd, string dataKind)
        {
            return await _repository.GetFixedToAttrMapsAsync(groupCompanyCd, dataKind);
        }

        /// リポジトリ経由で属性定義取得
        public async Task<List<MAttrDefinition>> GetAttrDefinitionsAsync()
        {
            return await _repository.GetAttrDefinitionsAsync();
        }

        /// 型付きCSV読み込み：再利用インスタンスでメモリ効率化
        private T Clone<T>(T source) where T : class, new()
        {
            var target = new T();
            var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
            foreach (var prop in properties)
            {
                if (prop.CanRead && prop.CanWrite)
                {
                    var value = prop.GetValue(source);
                    prop.SetValue(target, value);
                }
            }
            return target;
        }

        public async Task<List<T>> ReadCsvWithSettingsAsync<T>(string filePath, MDataImportSetting setting) where T : class, new()
        {
            ArgumentException.ThrowIfNullOrEmpty(filePath);
            var records = new List<T>();

            using var reader = new StreamReader(filePath, GetEncoding(setting.CharacterCd ?? "UTF-8"));
            using var csv = new CsvReader(reader, GetCsvConfiguration(setting));

            await SkipRowsAsync(csv, setting);

            var record = new T();
            await foreach (var _ in csv.EnumerateRecordsAsync(record))
            {
                records.Add(Clone(record));
            }

            return records;
        }

        /// 生CSV読み込み：文字列配列として処理
        public async Task<List<string[]>> ReadCsvRawAsync(string filePath, MDataImportSetting setting)
        {
            ArgumentException.ThrowIfNullOrEmpty(filePath);
            var rawData = new List<string[]>();

            using var reader = new StreamReader(filePath, GetEncoding(setting.CharacterCd ?? "UTF-8"));
            using var csv = new CsvReader(reader, GetCsvConfiguration(setting));

            await SkipRowsAsync(csv, setting);

            while (await csv.ReadAsync())
            {
                var record = new List<string>();
                for (int i = 0; csv.TryGetField<string>(i, out string? field); i++)
                {
                    record.Add(field ?? string.Empty);
                }
                rawData.Add(record.ToArray());
            }

            return rawData;
        }

        /// 文字コード取得：デフォルトUTF-8
        private static Encoding GetEncoding(string characterCd)
        {
            return characterCd?.ToUpperInvariant() switch
            {
                "UTF-8" => Encoding.UTF8,
                "SHIFT_JIS" => Encoding.GetEncoding("Shift_JIS"),
                "EUC-JP" => Encoding.GetEncoding("EUC-JP"),
                "GBK" => Encoding.GetEncoding("GBK"),
                _ => Encoding.UTF8
            };
        }

        /// CSV設定取得：区切り文字とヘッダー処理
        private static CsvConfiguration GetCsvConfiguration(MDataImportSetting setting)
        {
            return new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = setting.Delimiter ?? ",",
                HasHeaderRecord = setting.HeaderRowIndex > 0,
                MissingFieldFound = null,
                BadDataFound = null
            };
        }

        /// 行スキップ：ヘッダー前無効行を非同期処理
        private static async Task SkipRowsAsync(CsvReader csv, MDataImportSetting setting)
        {
            if (setting.HeaderRowIndex > 1)
            {
                for (int i = 1; i < setting.HeaderRowIndex; i++)
                {
                    await csv.ReadAsync();
                }
            }
        }
    }
}