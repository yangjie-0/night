using ProductDataIngestion.Models;

namespace ProductDataIngestion.Repositories.Interfaces
{
    public interface IClProductAttrRepository
    {
        Task<IEnumerable<ClProductAttr>> GetImportAttributesAsync(string batchId);
        Task UpdateProductAttrAsync(ClProductAttr entity);
        Task<IEnumerable<ClProductAttr>> CheckErrorAsync(string batchId);
        Task UpsertColorResultAsync(ClProductAttr entity);
    }
}
