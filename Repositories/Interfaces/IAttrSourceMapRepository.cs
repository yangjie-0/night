using ProductDataIngestion.Models;

namespace ProductDataIngestion.Repositories.Interfaces
{
    public interface IAttrSourceMapRepository
    {
        Task<long?> FindBySourceDataAsync(string? sourceId, string? sourceName);
    }
}
