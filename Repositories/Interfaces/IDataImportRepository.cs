using ProductDataIngestion.Models;

namespace ProductDataIngestion.Repositories.Interfaces
{
    public interface IDataImportRepository
    {
        Task<MDataImportSetting> GetImportSettingAsync(string groupCompanyCd, string usageNm);
        Task<List<MDataImportD>> GetImportDetailsAsync(long profileId);
        Task<List<MFixedToAttrMap>> GetFixedToAttrMapsAsync(string groupCompanyCd, string dataKind);
        Task<List<MAttrDefinition>> GetAttrDefinitionsAsync();
    }
}
