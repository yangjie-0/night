using System.Threading;
using ProductDataIngestion.Models;

namespace ProductDataIngestion.Repositories.Interfaces
{
    public interface IMBrandGRepository
    {
        Task<MBrandG?> GetByGBrandIdAsync(long id);
        Task<long?> GetIdByCodeAsync(string brandCode, CancellationToken cancellationToken);
    }
}
