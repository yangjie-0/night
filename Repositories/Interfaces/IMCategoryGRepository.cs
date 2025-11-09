using System.Threading;
using ProductDataIngestion.Models;

namespace ProductDataIngestion.Repositories.Interfaces
{
    public interface IMCategoryGRepository
    {
        Task<MCategoryG?> GetByIdAsync(long id);
        Task<long?> GetIdByCodeAsync(string categoryCode, CancellationToken cancellationToken);
    }
}
