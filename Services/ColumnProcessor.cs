using System.Text.Json;
using ProductDataIngestion.Models;
using System.Collections.Generic;
using System.Linq;

namespace ProductDataIngestion.Services
{
    // 列处理服务：处理 m_data_import_d 逻辑。
    public class ColumnProcessor
    {
        private readonly DataImportService _dataService;

        public ColumnProcessor(DataImportService dataService)
        {
            _dataService = dataService;
        }

        // 处理单列逻辑（if/else if/else）。
        public async Task ProcessColumnAsync(
            MDataImportD detail, string? rawValue, string? transformedValue,
            TempProductParsed tempProduct, Dictionary<string, object> extrasDict,
            List<ClProductAttr> productAttrs, string batchId, string groupCompanyCd, Guid tempRowId)
        {
            // 备份所有内容到 extras_json
            extrasDict[$"col_{detail.ColumnSeq}"] = new
            {
                header = "N/A", // headers 从外部传入
                raw_value = rawValue ?? "",
                transformed_value = transformedValue ?? "",
                attr_cd = detail.AttrCd ?? string.Empty,
                target_column = detail.TargetColumn ?? string.Empty,
                target_entity = detail.TargetEntity ?? string.Empty,
                transform_expr = detail.TransformExpr ?? string.Empty,
                is_required = detail.IsRequired,
                processing_stage = "INGEST"
            };

            // if: target_column 有内容 → 登录到 temp_product_parsed 固定字段 (source_ 前缀)
            if (!string.IsNullOrEmpty(detail.TargetColumn))
            {
                string targetFieldName = "source_" + detail.TargetColumn.ToLower(); // e.g., source_product_cd
                if (SetPropertyValue(tempProduct, targetFieldName, transformedValue))
                {
                    Console.WriteLine($"    → 固定フィールド: {targetFieldName} = {transformedValue}");
                }
                return;
            }

            // else if: attr_cd 有内容 → EAV 项目，使用 m_fixed_to_attr_map 映射，直接存入 cl_product_attr
            if (!string.IsNullOrEmpty(detail.AttrCd) && string.IsNullOrWhiteSpace(transformedValue) == false)
            {
                var attrMaps = await _dataService.GetFixedToAttrMapsAsync(groupCompanyCd, "PRODUCT");
                var attrMap = attrMaps.FirstOrDefault(m => m.AttrCd == detail.AttrCd);

                var productAttr = new ClProductAttr
                {
                    BatchId = batchId,
                    TempRowId = tempRowId,
                    AttrCd = detail.AttrCd,
                    AttrSeq = (short)(productAttrs.Count(p => p.TempRowId == tempRowId) + 1), // 简单序号
                    SourceId = attrMap?.SourceIdColumn ?? "",
                    SourceLabel = attrMap?.SourceLabelColumn ?? "",
                    SourceRaw = transformedValue ?? "",
                    DataType = attrMap?.DataTypeOverride ?? "TEXT",
                    QualityFlag = "OK",
                    QualityDetailJson = "{}",
                    ProvenanceJson = JsonSerializer.Serialize(new
                    {
                        stage = "INGEST",
                        from = "EAV",
                        via = attrMap != null ? "fixed_map" : "direct_map",
                        profile_id = detail.ProfileId,
                        column_seq = detail.ColumnSeq,
                        map_id = attrMap?.MapId
                    }),
                    RuleVersion = "1.0"
                };

                productAttrs.Add(productAttr);
                Console.WriteLine($"    → EAV属性生成 (map): {detail.AttrCd} = {transformedValue} (source_id={attrMap?.SourceIdColumn})");
                return;
            }

            // else: 仅备份到 extras_json (已在上方处理)
            Console.WriteLine($"    → 仅备份: col_{detail.ColumnSeq} to extras_json");
        }

        // 辅助：设置 TempProductParsed 属性 (反射)。
        private bool SetPropertyValue(TempProductParsed obj, string propertyName, string? value)
        {
            try
            {
                var property = typeof(TempProductParsed).GetProperty(
                    propertyName,
                    System.Reflection.BindingFlags.IgnoreCase | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance
                );
                
                if (property != null && property.CanWrite)
                {
                    property.SetValue(obj, value);
                    return true;
                }
                return false;
            }
            catch
            {
                return false;
            }
        }
    }
}