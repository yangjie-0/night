using ProductDataIngestion.Models;

namespace ProductDataIngestion.Repositories.Interfaces
{
    // 商品データのリポジトリインターフェース
    public interface IProductRepository
    {
        // 一時商品データを一括保存
        Task SaveTempProductsAsync(List<TempProductParsed> products);

        // 一時イベントデータを一括保存
        Task SaveTempProductEventsAsync(List<TempProductEvent> events);

        // idem_key = (batch_id, time_no) の重複存在チェック
        Task<bool> EventIdemKeyExistsAsync(string batchId, long timeNo);

        // 商品属性データを一括保存
        Task SaveProductAttributesAsync(List<ClProductAttr> attributes);

        // エラーレコードを一括保存
        Task SaveRecordErrorsAsync(List<RecordError> errors);
    }
}
