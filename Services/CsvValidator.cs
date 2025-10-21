using ProductDataIngestion.Models;

namespace ProductDataIngestion.Services
{
    /// <summary>
    /// CSV検証ロジッククラス
    /// IngestServiceから検証ロジックを分離
    /// </summary>
    public class CsvValidator
    {
        /// <summary>
        /// 列マッピング検証
        /// column_seq = 0: 公司コード注入 (CSV列不要)
        /// column_seq > 0: CSV列番号 (1始まり、配列インデックスは -1 が必要)
        /// 重要: is_required=true の必須列のみ検証し、オプション列は CSV に存在しなくても許可
        /// </summary>
        public void ValidateColumnMappings(List<MDataImportD> importDetails, string[] headers)
        {
            var errors = new List<string>();
            var requiredCount = 0;

            foreach (var detail in importDetails
                .Where(d => d.IsRequired)
                .OrderBy(d => d.ColumnSeq))
            {
                // column_seq = 0 は公司コード注入なのでスキップ
                if (detail.ColumnSeq == 0) continue;

                // column_seq > 0 は CSV列番号 (1始まり)、配列インデックスは -1
                int csvIndex = detail.ColumnSeq - 1;

                // CSV範囲外チェック: 必須列のみエラーとする
                if (csvIndex < 0 || csvIndex >= headers.Length)
                {
                    if (detail.IsRequired)
                    {
                        errors.Add($"必須列{detail.ColumnSeq} ({detail.AttrCd ?? detail.TargetColumn}) がCSV範囲外 (CSV列数: {headers.Length})");
                        requiredCount++;
                    }
                }
            }

            Console.WriteLine($"列マッピング検証完了: CSV列数={headers.Length}, 必須列エラー={requiredCount}");

            if (errors.Any())
            {
                throw new IngestException(
                    ErrorCodes.MISSING_COLUMN,
                    $"列マッピングエラー:\n{string.Join("\n", errors)}"
                );
            }
        }

        /// <summary>
        /// 必須フィールドの検証
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
