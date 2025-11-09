using ProductDataIngestion.Models;

namespace ProductDataIngestion.Repositories.Interfaces
{
    public interface IRefTableMapRepository
    {
        Task<RefTableMap?> GetByCodeAsync(string attrCd);
        Task<IEnumerable<RefTableMap>> GetAllAsync();
    }
}
