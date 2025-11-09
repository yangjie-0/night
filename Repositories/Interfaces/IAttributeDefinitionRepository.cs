using ProductDataIngestion.Models;

namespace ProductDataIngestion.Repositories.Interfaces
{
    public interface IAttributeDefinitionRepository
    {
        Task<AttributeDefinition?> GetByCodeAsync(string attrCd);
        Task<IEnumerable<AttributeDefinition>> GetAllAttrDefinitionAsync();


    }
}
