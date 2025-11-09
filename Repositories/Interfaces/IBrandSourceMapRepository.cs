using ProductDataIngestion.Models;

namespace ProductDataIngestion.Repositories.Interfaces
{
    public interface IBrandSourceMapRepository
    {
        Task<long?> FindBySourceDataAsync(string? sourceId);
    }
}
