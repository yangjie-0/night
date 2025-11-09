using System.Collections.Generic;
using System.Threading.Tasks;
using ProductDataIngestion.Models;

namespace ProductDataIngestion.Repositories.Interfaces
{
    public interface IRecordErrorRepository
    {
        /// <summary>
        /// 插入一条 record_error 记录
        /// </summary>
        Task InsertAsync(RecordError error);

        /// <summary>
        /// 根据 batch_id 查询错误列表（用于调试/展示）
        /// </summary>
        Task<IEnumerable<RecordError>> GetByBatchIdAsync(string batchId);
    }
}
