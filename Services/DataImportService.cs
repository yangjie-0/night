using ProductDataIngestion.Models;
using Npgsql;
using Dapper;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using System.Text;

namespace ProductDataIngestion.Services
{
    public class DataImportService
    {
        private readonly string _connectionString;

        public DataImportService(string connectionString)
        {
            _connectionString = connectionString;
        }

        // 同步方法
        public MDataImportSetting GetImportSetting(string groupCompanyCd, string usageNm)
        {
            return GetImportSettingAsync(groupCompanyCd, usageNm).GetAwaiter().GetResult();
        }

        public List<MDataImportD> GetImportDetails(long profileId)
        {
            return GetImportDetailsAsync(profileId).GetAwaiter().GetResult();
        }

        public List<MFixedToAttrMap> GetFixedToAttrMaps(string groupCompanyCd, string dataKind)
        {
            return GetFixedToAttrMapsAsync(groupCompanyCd, dataKind).GetAwaiter().GetResult();
        }

        // 异步方法
        public async Task<MDataImportSetting> GetImportSettingAsync(string groupCompanyCd, string usageNm)
        {
            using var connection = new NpgsqlConnection(_connectionString);
            
            const string sql = @"
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

            var setting = await connection.QueryFirstOrDefaultAsync<MDataImportSetting>(sql, new 
            { 
                GroupCompanyCd = groupCompanyCd, 
                UsageNm = usageNm 
            });

            if (setting == null)
            {
                throw new Exception($"設定が見つかりません: GP会社コード={groupCompanyCd}, 用途名={usageNm}");
            }

            return setting;
        }

        public async Task<List<MDataImportD>> GetImportDetailsAsync(long profileId)
        {
            using var connection = new NpgsqlConnection(_connectionString);
            
            const string sql = @"
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

            return (await connection.QueryAsync<MDataImportD>(sql, new { ProfileId = profileId })).ToList();
        }

        public async Task<List<MFixedToAttrMap>> GetFixedToAttrMapsAsync(string groupCompanyCd, string dataKind)
        {
            using var connection = new NpgsqlConnection(_connectionString);
            
            const string sql = @"
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

            return (await connection.QueryAsync<MFixedToAttrMap>(sql, new 
            { 
                GroupCompanyCd = groupCompanyCd, 
                DataKind = dataKind 
            })).ToList();
        }

        // CSV 相关方法
        public async Task<List<T>> ReadCsvWithSettingsAsync<T>(string filePath, MDataImportSetting setting) where T : class
        {
            var records = new List<T>();
            
            using var reader = new StreamReader(filePath, GetEncoding(setting.CharacterCd));
            using var csv = new CsvReader(reader, GetCsvConfiguration(setting));

            await SkipRowsAsync(csv, setting);
            records = csv.GetRecords<T>().ToList();
            
            return records;
        }

        public async Task<List<string[]>> ReadCsvRawAsync(string filePath, MDataImportSetting setting)
        {
            var rawData = new List<string[]>();
            
            using var reader = new StreamReader(filePath, GetEncoding(setting.CharacterCd));
            using var csv = new CsvReader(reader, GetCsvConfiguration(setting));

            await SkipRowsAsync(csv, setting);

            while (await csv.ReadAsync())
            {
                var record = new List<string>();
                for (int i = 0; csv.TryGetField<string>(i, out string field); i++)
                {
                    record.Add(field);
                }
                rawData.Add(record.ToArray());
            }
            
            return rawData;
        }

        private Encoding GetEncoding(string characterCd)
        {
            return characterCd?.ToUpper() switch
            {
                "UTF-8" => Encoding.UTF8,
                "SHIFT_JIS" => Encoding.GetEncoding("Shift_JIS"),
                "EUC-JP" => Encoding.GetEncoding("EUC-JP"),
                _ => Encoding.UTF8
            };
        }

        private CsvConfiguration GetCsvConfiguration(MDataImportSetting setting)
        {
            return new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = setting.Delimiter ?? ",",
                HasHeaderRecord = setting.HeaderRowIndex > 0,
                MissingFieldFound = null,
                BadDataFound = null
            };
        }

        private async Task SkipRowsAsync(CsvReader csv, MDataImportSetting setting)
        {
            if (setting.HeaderRowIndex > 1)
            {
                for (int i = 1; i < setting.HeaderRowIndex; i++)
                {
                    await csv.ReadAsync();
                }
            }

            for (int i = 0; i < setting.SkipRowCount; i++)
            {
                await csv.ReadAsync();
            }
        }
    }
}