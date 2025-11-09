using ProductDataIngestion.Models;

namespace ProductDataIngestion.Repositories.Interfaces
{
    public interface IMCleanseRuleSetRepository
    {
        Task<MCleanseRuleSet?> GetByIdAsync(long ruleSetId);
        Task<IEnumerable<MCleanseRuleSet>> GetAllAsync();
    }
}
