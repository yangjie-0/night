using ProductDataIngestion.Models;

namespace ProductDataIngestion.Repositories.Interfaces
{
    /// <summary>
    /// データ取り込みに関するDBアクセスを抽象化するリポジトリインターフェース。
    /// m_data_import_setting / m_data_import_d / m_fixed_to_attr_map / m_attr_definition などを取得するための契約を定義する。
    /// </summary>
    public interface IDataImportRepository
    {
        /// <summary>
        /// 指定したグループ会社とユースケースに対応するインポート設定を取得する。
        /// </summary>
        Task<MDataImportSetting> GetImportSettingAsync(string groupCompanyCd, string usageNm);

        /// <summary>
        /// 指定のグループ会社コードと対象エンティティに対して、アクティブなインポート設定を全件取得する。
        /// 重複チェックや単一選定は上位層で行う。
        /// </summary>
        Task<List<MDataImportSetting>> GetActiveImportSettingsAsync(string groupCompanyCd, string targetEntity);

        /// <summary>
        /// プロファイルIDに紐づくインポート明細（カラム定義）を取得する。
        /// </summary>
        Task<List<MDataImportD>> GetImportDetailsAsync(long profileId);

        /// <summary>
        /// 固定列 -> 属性マッピング定義を取得する（groupCompanyCd と projectionKind によるフィルタ）。
        /// </summary>
        Task<List<MFixedToAttrMap>> GetFixedToAttrMapsAsync(string groupCompanyCd, string projectionKind);

        /// <summary>
        /// 属性定義（m_attr_definition）を全件取得する。
        /// </summary>
        Task<List<MAttrDefinition>> GetAttrDefinitionsAsync();
    }
}
