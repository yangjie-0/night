// Repositories/DataImportRepository.cs
// データインポートリポジトリ実装：DBアクセスロジックを分離

using ProductDataIngestion.Models;
using ProductDataIngestion.Services;
using Npgsql;
using Dapper;
using Microsoft.Extensions.Logging;
using ProductDataIngestion.Repositories.Interfaces;  // IDataImportRepository用


namespace ProductDataIngestion.Repositories
{
    /// SQLクエリ定数：メンテナンス性を向上
    static class SqlQueries
    {
        /// インポート設定取得SQL
        public const string GetImportSetting = @"
            SELECT 
                profile_id as ProfileId,
                usage_nm as UsageNm,
                group_company_cd as GroupCompanyCd,
                target_entity as TargetEntity,
                character_cd as CharacterCd,
                delimiter as Delimiter,
                header_row_index as HeaderRowIndex,
                is_active as IsActive,
                import_setting_remarks as ImportSettingRemarks,
                cre_at as CreAt,
                upd_at as UpdAt
            FROM m_data_import_setting 
            WHERE group_company_cd = @GroupCompanyCd 
                AND usage_nm = @UsageNm 
                AND is_active = true";

        /// アクティブなインポート設定を group_company_cd / target_entity で取得
        public const string GetActiveImportSettingsByEntity = @"
            SELECT 
                profile_id as ProfileId,
                usage_nm as UsageNm,
                group_company_cd as GroupCompanyCd,
                target_entity as TargetEntity,
                character_cd as CharacterCd,
                delimiter as Delimiter,
                header_row_index as HeaderRowIndex,
                is_active as IsActive,
                import_setting_remarks as ImportSettingRemarks,
                cre_at as CreAt,
                upd_at as UpdAt
            FROM m_data_import_setting 
            WHERE group_company_cd = @GroupCompanyCd 
                AND target_entity = @TargetEntity 
                AND is_active = true
            ORDER BY profile_id";

        /// インポート明細取得SQL
        public const string GetImportDetails = @"
            SELECT 
                profile_id as ProfileId,
                column_seq as ColumnSeq,
                projection_kind as ProjectionKind,
                attr_cd as AttrCd,
                target_column as TargetColumn,
                cast_type as CastType,
                transform_expr as TransformExpr,
                is_required as IsRequired
            FROM m_data_import_d 
            WHERE profile_id = @ProfileId 
            ORDER BY column_seq";

        /// 固定属性マッピング取得SQL
        public const string GetFixedToAttrMaps = @"
            SELECT
                map_id as MapId,
                group_company_cd as GroupCompanyCd,
                projection_kind as ProjectionKind,
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
                AND projection_kind = @ProjectionKind
                AND is_active = true
            ORDER BY priority ASC, map_id ASC";

        /// 属性定義取得SQL
        public const string GetAttrDefinitions = @"
            SELECT
                attr_id as AttrId,
                attr_cd as AttrCd,
                attr_nm as AttrNm,
                attr_sort_no as AttrSortNo,
                g_category_cd as GCategoryCd,
                data_type as DataType,
                g_list_group_cd as GListGroupCd,
                select_type as SelectType,
                is_golden_attr as IsGoldenAttr,
                cleanse_phase as CleansePhase,
                required_context_keys as RequiredContextKeys,
                target_table as TargetTable,
                target_column as TargetColumn,
                product_unit_cd as ProductUnitCd,
                credit_active_flag as CreditActiveFlag,
                usage as Usage,
                table_type_cd as TableTypeCd,
                is_golden_product as IsGoldenProduct,
                is_golden_attr as IsGoldenAttrEav,
                is_active as IsActive,
                attr_remarks as AttrRemarks,
                cre_at as CreAt,
                upd_at as UpdAt
            FROM m_attr_definition
            WHERE is_active = true
            ORDER BY attr_sort_no NULLS LAST, attr_id";
    }

    /// <summary>
    /// データ取り込みに関するDBアクセス実装。
    /// m_data_import_setting, m_data_import_d, m_fixed_to_attr_map, m_attr_definition などの取得を行う。
    /// </summary>
    public class DataImportRepository : IDataImportRepository
    {
        private readonly string _connectionString;
        private readonly ILogger<DataImportRepository>? _logger;

        /// <summary>
        /// コンストラクタ：接続文字列と任意のロガーを受け取る。
        /// </summary>
        public DataImportRepository(string connectionString, ILogger<DataImportRepository>? logger = null)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _logger = logger;
        }

        /// <summary>
        /// 指定した groupCompanyCd と usageNm に対応するインポート設定を取得する。
        /// 見つからない場合は ImportException を投げる。
        /// </summary>
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

        /// <summary>
        /// 指定の group_company_cd / target_entity に対するアクティブ設定を全件取得する。
        /// </summary>
        public async Task<List<MDataImportSetting>> GetActiveImportSettingsAsync(string groupCompanyCd, string targetEntity)
        {
            using var connection = new NpgsqlConnection(_connectionString);
            return (await connection.QueryAsync<MDataImportSetting>(
                SqlQueries.GetActiveImportSettingsByEntity,
                new { GroupCompanyCd = groupCompanyCd, TargetEntity = targetEntity }
            )).ToList();
        }

        /// <summary>
        /// 指定プロファイルIDに紐づくインポート明細（カラム定義）を取得する。
        /// </summary>
        public async Task<List<MDataImportD>> GetImportDetailsAsync(long profileId)
        {
            using var connection = new NpgsqlConnection(_connectionString);
            return (await connection.QueryAsync<MDataImportD>(
                SqlQueries.GetImportDetails,
                new { ProfileId = profileId })).ToList();
        }

        /// <summary>
        /// 固定列から属性へのマッピング定義を取得する。
        /// groupCompanyCd と projectionKind でフィルタする。
        /// </summary>
        public async Task<List<MFixedToAttrMap>> GetFixedToAttrMapsAsync(string groupCompanyCd, string projectionKind)
        {
            using var connection = new NpgsqlConnection(_connectionString);
            return (await connection.QueryAsync<MFixedToAttrMap>(
                SqlQueries.GetFixedToAttrMaps,
                new { GroupCompanyCd = groupCompanyCd, ProjectionKind = projectionKind })).ToList();
        }

        /// <summary>
        /// 属性定義（m_attr_definition）を取得する。
        /// </summary>
        public async Task<List<MAttrDefinition>> GetAttrDefinitionsAsync()
        {
            using var connection = new NpgsqlConnection(_connectionString);
            return (await connection.QueryAsync<MAttrDefinition>(
                SqlQueries.GetAttrDefinitions)).ToList();
        }
    }
}
