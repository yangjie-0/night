using ProductDataIngestion.Models;

namespace ProductDataIngestion.Repositories.Interfaces
{
    public interface IMCompanyRepository
    {
        Task<MCompany?> FindBySourceDataAsync(string? SourceId);
    }
}
