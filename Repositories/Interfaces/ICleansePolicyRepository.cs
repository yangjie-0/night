using ProductDataIngestion.Models;

namespace ProductDataIngestion.Repositories.Interfaces
{
    public interface ICleansePolicyRepository
    {
        Task<IEnumerable<CleansePolicy>> GetAllAsync();
        Task<CleansePolicy?> GetByCodeAsync(string attrCd);
        Task<CleansePolicy?> GetPolicyAsync(string attrCd, string? groupCompanyCd, string? brand, string? GCategoryCd);
        Task<IEnumerable<CleansePolicy>> GetPoliciesAsync(string attrCd, string? groupCompanyCd);
    }
}
