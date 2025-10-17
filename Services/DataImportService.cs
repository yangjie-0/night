using ProductDataIngestion.Models;
using Npgsql;
using Dapper;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using System.Text;
using Microsoft.Extensions.Logging; // 添加日志依赖
using System.Reflection;

namespace ProductDataIngestion.Services
{
    /// 自定义导入异常类，用于统一处理数据导入相关错误。
    public class ImportException : Exception
    {
        // 使用指定消息初始化异常。
        // <param name="message">异常消息。</param>
        public ImportException(string message) : base(message) { }

        // 使用指定消息和内部异常初始化异常。
        // <param name="message">异常消息。</param>
        // <param name="inner">内部异常。</param>
        public ImportException(string message, Exception inner) : base(message, inner) { }
    }
    // 数据导入服务接口，定义所有核心操作。
    public interface IDataImportService
    {
        // 异步获取导入设置。
        // <param name="groupCompanyCd">集团公司代码。</param>
        // <param name="usageNm">用途名称。</param>
        // <returns>导入设置对象。</returns>
        Task<MDataImportSetting> GetImportSettingAsync(string groupCompanyCd, string usageNm);
        // 异步获取导入明细。
        // <param name="profileId">配置文件ID。</param>
        // <returns>导入明细列表。</returns>
        Task<List<MDataImportD>> GetImportDetailsAsync(long profileId);
        // 异步获取固定属性映射。
        // <param name="groupCompanyCd">集团公司代码。</param>
        // <param name="dataKind">数据种类。</param>
        // <returns>固定属性映射列表。</returns>
        Task<List<MFixedToAttrMap>> GetFixedToAttrMapsAsync(string groupCompanyCd, string dataKind);

        // 异步读取CSV文件为类型化记录列表。
        // <typeparam name="T">记录类型，必须为类。</typeparam>
        // <param name="filePath">文件路径。</param>
        // <param name="setting">导入设置。</param>
        // <returns>类型化记录列表。</returns>
        Task<List<T>> ReadCsvWithSettingsAsync<T>(string filePath, MDataImportSetting setting) where T : class, new(); // 添加 new() 约束

        // 异步读取CSV文件为原始字符串数组列表。
        // <param name="filePath">文件路径。</param>
        // <param name="setting">导入设置。</param>
        // <returns>原始行数据列表。</returns>
        Task<List<string[]>> ReadCsvRawAsync(string filePath, MDataImportSetting setting);
    }

    // 数据导入服务实现类，提供数据库查询和CSV文件读取功能。
    public class DataImportService : IDataImportService
    {
        private readonly string _connectionString;
        private readonly ILogger<DataImportService>? _logger; // 可选注入日志

        // 使用连接字符串初始化服务。
        // <param name="connectionString">数据库连接字符串。</param>
        // <param name="logger">日志记录器（可选）。</param>
        public DataImportService(string connectionString, ILogger<DataImportService>? logger = null)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _logger = logger;
        }

        // 以下为原有同步方法，保留接口名，添加ConfigureAwait(false)以优化异步上下文
        // 同步获取导入设置（内部调用异步方法）。

        public MDataImportSetting GetImportSetting(string groupCompanyCd, string usageNm)
        {
            return GetImportSettingAsync(groupCompanyCd, usageNm).ConfigureAwait(false).GetAwaiter().GetResult();
        }
        // 同步获取导入明细（内部调用异步方法）。

        public List<MDataImportD> GetImportDetails(long profileId)
        {
            return GetImportDetailsAsync(profileId).ConfigureAwait(false).GetAwaiter().GetResult();
        }
        // 同步获取固定属性映射（内部调用异步方法）。
        public List<MFixedToAttrMap> GetFixedToAttrMaps(string groupCompanyCd, string dataKind)
        {
            return GetFixedToAttrMapsAsync(groupCompanyCd, dataKind).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        // SQL常量类，提取查询语句以提高可维护性
        private static class SqlQueries
        {
            // 获取导入设置的SQL查询。
            public const string GetImportSetting = @"
                SELECT 
                    profile_id as ProfileId,
                    usage_nm as UsageNm,
                    group_company_cd as GroupCompanyCd,
                    target_entity as TargetEntity,
                    character_cd as CharacterCd,
                    delimiter as Delimiter,
                    header_row_index as HeaderRowIndex,
                    skip_row_count as SkipRowCount,
                    is_active as IsActive,
                    import_setting_remarks as ImportSettingRemarks,
                    cre_at as CreAt,
                    upd_at as UpdAt
                FROM m_data_import_setting 
                WHERE group_company_cd = @GroupCompanyCd 
                    AND usage_nm = @UsageNm 
                    AND is_active = true";

            // 获取导入明细的SQL查询。
            public const string GetImportDetails = @"
                SELECT 
                    profile_id as ProfileId,
                    column_seq as ColumnSeq,
                    target_entity as TargetEntity,
                    attr_cd as AttrCd,
                    target_column as TargetColumn,
                    cast_type as CastType,
                    transform_expr as TransformExpr,
                    is_required as IsRequired
                FROM m_data_import_d 
                WHERE profile_id = @ProfileId
                ORDER BY column_seq";

            // 获取固定属性映射的SQL查询。
            public const string GetFixedToAttrMaps = @"
                SELECT 
                    map_id as MapId,
                    group_company_cd as GroupCompanyCd,
                    data_kind as DataKind,
                    attr_cd as AttrCd,
                    source_id_column as SourceIdColumn,
                    source_label_column as SourceLabelColumn,
                    value_role as ValueRole,
                    data_type_override as DataTypeOverride,
                    split_mode as SplitMode,
                    is_active as IsActive,
                    priority as Priority,
                    fixed_remarks as FixedRemarks
                FROM m_fixed_to_attr_map 
                WHERE group_company_cd = @GroupCompanyCd 
                    AND data_kind = @DataKind 
                    AND is_active = true
                ORDER BY priority";
        }

        // 异步从数据库获取导入设置。
        public async Task<MDataImportSetting> GetImportSettingAsync(string groupCompanyCd, string usageNm)
        {
            using var connection = new NpgsqlConnection(_connectionString);
            var setting = await connection.QueryFirstOrDefaultAsync<MDataImportSetting>(
                SqlQueries.GetImportSetting,
                new { GroupCompanyCd = groupCompanyCd, UsageNm = usageNm });

            if (setting == null)
            {
                var msg = $"Import setting not found: GroupCompanyCd={groupCompanyCd}, UsageNm={usageNm}";
                _logger?.LogError(msg);
                throw new ImportException(msg);
            }

            return setting;
        }
        // 异步从数据库获取导入明细列表。
        public async Task<List<MDataImportD>> GetImportDetailsAsync(long profileId)
        {
            using var connection = new NpgsqlConnection(_connectionString);
            return (await connection.QueryAsync<MDataImportD>(
                SqlQueries.GetImportDetails,
                new { ProfileId = profileId })).ToList();
        }

        // 异步从数据库获取固定属性映射列表。
        public async Task<List<MFixedToAttrMap>> GetFixedToAttrMapsAsync(string groupCompanyCd, string dataKind)
        {
            using var connection = new NpgsqlConnection(_connectionString);
            return (await connection.QueryAsync<MFixedToAttrMap>(
                SqlQueries.GetFixedToAttrMaps,
                new { GroupCompanyCd = groupCompanyCd, DataKind = dataKind })).ToList();
        }

        // CSV相关方法，保留原有接口名，优化为全异步流式处理

        // 异步读取CSV文件为类型化记录列表，支持配置设置。
        // DataImportService 实现：保持 where T : class, new()
        // 在 DataImportService 类中添加私有克隆辅助方法
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

// 更新 ReadCsvWithSettingsAsync 方法
public async Task<List<T>> ReadCsvWithSettingsAsync<T>(string filePath, MDataImportSetting setting) where T : class, new()
{
    ArgumentException.ThrowIfNullOrEmpty(filePath);
    var records = new List<T>();

    using var reader = new StreamReader(filePath, GetEncoding(setting.CharacterCd ?? "UTF-8"));
    using var csv = new CsvReader(reader, GetCsvConfiguration(setting));

    await SkipRowsAsync(csv, setting);

    // 创建重用实例
    var record = new T();
    await foreach (var _ in csv.EnumerateRecordsAsync(record))
    {
        // 添加当前行的record副本（反射浅拷贝）
        records.Add(Clone(record));
    }

    return records;
}
        // 异步读取CSV文件为原始字符串数组列表，支持配置设置。
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

        // 根据字符代码获取文件编码。
        private static Encoding GetEncoding(string characterCd)
        {
            return characterCd.ToUpperInvariant() switch
            {
                "UTF-8" => Encoding.UTF8,
                "SHIFT_JIS" => Encoding.GetEncoding("Shift_JIS"),
                "EUC-JP" => Encoding.GetEncoding("EUC-JP"),
                "GBK" => Encoding.GetEncoding("GBK"), // 添加中文支持
                _ => Encoding.UTF8
            };
        }
        //根据设置获取CSV配置。
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

        //异步跳过CSV文件中的指定行（头行和跳过行）。
        private static async Task SkipRowsAsync(CsvReader csv, MDataImportSetting setting)
        {
            // 跳过头前无效行，使头行成为当前行
            if (setting.HeaderRowIndex > 1)
            {
                for (int i = 1; i < setting.HeaderRowIndex; i++)
                {
                    await csv.ReadAsync();
                }
            }

            // 跳过额外行（直接使用 SkipRowCount，默认0）
            for (int i = 0; i < setting.SkipRowCount; i++)
            {
                await csv.ReadAsync();
            }
        }
    }
}