using ProductDataIngestion.Models;

namespace ProductDataIngestion.Repositories.Interfaces
{
    public interface ICategorySourceMapRepository
    {
        Task<long?> FindByCategoryAsync(string attrCd, string? sourceId);
    }
}
