using ProductDataIngestion.Models;

namespace ProductDataIngestion.Repositories.Interfaces
{
    public interface IMListItemGRepository
    {
        Task<MListItemG?> GetByListItemIdAsync(long id);
        Task<IEnumerable<MListItemG>> GetAllAsync();
    }
}
