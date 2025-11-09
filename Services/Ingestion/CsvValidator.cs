using System;
using System.Collections.Generic;
using System.Linq;
using ProductDataIngestion.Models;

namespace ProductDataIngestion.Services
{
    /// <summary>
    /// CSV検証ロジッククラス  
    /// IngestService / CsvTempIngestionService から呼び出され、  
    /// 列数・必須項目・空行などの共通検証を担当する。
    /// </summary>
    public class CsvValidator
    {
        /// <summary>
        /// CSVファイルの列数を基に、必須列不足を検証する。
        /// 中文说明：根据 CSV 实际列数，检查 m_data_import_d 中的必填列是否超出范围。
        /// </summary>
        public void ValidateColumnCount(List<MDataImportD> importDetails, int columnCount)
        {
            var errors = new List<string>();

            // is_required=true の列のみをチェック
            foreach (var detail in importDetails.Where(d => d.IsRequired))
            {
                // 定義された column_seq が CSV実際の列数を超えている場合
                if (detail.ColumnSeq > columnCount)
                {
                    string attrInfo = !string.IsNullOrEmpty(detail.AttrCd)
                        ? $"[{detail.AttrCd}]"
                        : $"[{detail.TargetColumn}]";

                    errors.Add($"必須列が不足しています: 定義 {detail.ColumnSeq}列目 {attrInfo}、CSV推定列数={columnCount}");
                }
            }

            if (errors.Any())
            {
                throw new IngestException(
                    ErrorCodes.MISSING_COLUMN,
                    $"CSVファイルの列数が不足しています。定義に対して列が足りません。\n{string.Join("\n", errors)}"
                );
            }

            Console.WriteLine($"列数検証完了: 推定列数={columnCount}, 必須列エラー={errors.Count}");
        }

        /// <summary>
        /// 必須フィールドの検証  
        /// 中文说明：检查一行中是否存在空的必填字段。
        /// </summary>
        public void ValidateRequiredFields(List<string> requiredFieldErrors, long dataRowNumber, int physicalLine)
        {
            if (requiredFieldErrors.Any())
            {
                throw new IngestException(
                    ErrorCodes.MISSING_COLUMN,
                    $"必須項目エラー: {string.Join(", ", requiredFieldErrors)}",
                    recordRef: $"line:{dataRowNumber}",
                    rawFragment: $"物理行:{physicalLine}"
                );
            }
        }

        /// <summary>
        /// 空レコードの検証  
        /// 中文说明：检测是否为空行。
        /// </summary>
        public void ValidateEmptyRecord(string[]? record, long dataRowNumber, int physicalLine)
        {
            if (record == null || record.Length == 0)
            {
                throw new IngestException(
                    ErrorCodes.PARSE_FAILED,
                    "空のレコード",
                    recordRef: $"line:{dataRowNumber}",
                    rawFragment: $"物理行:{physicalLine}"
                );
            }
        }
    }
}
