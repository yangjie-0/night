using System;
using System.Threading.Tasks;
using ProductDataIngestion.Models;
using ProductDataIngestion.Utils;
using ProductDataIngestion.Repositories.Interfaces;

namespace ProductDataIngestion.Utils
{
    /// <summary>
    /// クレンジング結果（成功・警告・エラー）を統一的に登録・更新するヘルパークラス。
    /// </summary>
    public static class CleanseResultHelper
    {
        public static async Task HandleResultAsync(
            IRecordErrorRepository recordErrorRepo,
            IClProductAttrRepository productAttrRepo,
            ClProductAttr attr,
            BatchRun batchRun,
            CleansePolicy? policy,
            string qualityStatus,
            string message,
            string workerId,// provenance_json
            string? errorCode = null,
            string? errorDetail = null,
            string? reasonCd = null,
            string? valueCd = null,
            string? valueText = null,
            decimal? valueNum = null,
            DateTime? valueDate = null,
            bool updateAttr = true)
        {
            // record_error（NG時のみ）
            if (qualityStatus == "NG" || qualityStatus == "WARN")
            {
                var error = new RecordError
                {
                    ErrorId = Guid.NewGuid(),
                    BatchId = batchRun.BatchId,
                    Step = "CLEANSE",
                    RecordRef = $"temp_row_id={attr.TempRowId}",
                    ErrorCd = errorCode,
                    ErrorDetail = errorDetail ?? message,
                    RawFragment = attr.SourceRaw ?? string.Empty,
                    CreAt = DateTime.UtcNow,
                    UpdAt = DateTime.UtcNow
                };
                await recordErrorRepo.InsertAsync(error);
            }

            // value系設定（成功時）
            attr.ValueCd = valueCd ?? attr.ValueCd;
            attr.ValueText = valueText ?? attr.ValueText;
            attr.ValueNum = valueNum ?? attr.ValueNum;
            attr.ValueDate = valueDate ?? attr.ValueDate;

            // quality_detail_json
            attr.QualityStatus = qualityStatus;
            attr.QualityDetailJson = QualityLogHelper.BuildQualityDetail(
                result: attr.QualityStatus,
                reasonCd: reasonCd ?? policy?.MatcherKind ?? "UNKNOWN",
                message: message,
                sourceRaw: attr.SourceRaw,
                valueCd: attr.ValueCd,
                valueText: attr.ValueText,
                valueNum: attr.ValueNum,
                valueDate: attr.ValueDate
            );

            //　provenance_json
            var prov = QualityLogHelper.BuildProvenance(
                ruleSetId: policy?.RuleSetId ?? 0,
                ruleVersion: attr.RuleVersion,
                policyId: policy?.PolicyId ?? 0,
                attrCd: attr.AttrCd,
                matcherKind: policy?.MatcherKind ?? "UNKNOWN",
                stepNo: policy?.StepNo ?? 0,
                sourceRaw: attr.SourceRaw,
                groupCompanyCd: batchRun.GroupCompanyCd,
                batchId: batchRun.BatchId,
                tempRowId: attr.TempRowId.ToString(),
                workerId: workerId
            );

            attr.ProvenanceJson = ProvenanceHelper.AppendProvenanceJson(attr.ProvenanceJson, prov);

            // DB更新（必要に応じて）
            if (updateAttr)
                await productAttrRepo.UpdateProductAttrAsync(attr);
        }
    }
}
