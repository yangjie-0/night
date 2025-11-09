using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace ProductDataIngestion.Utils
{
    /// <summary>
    /// クレンジング結果（quality_detail_json / provenance_json）を生成するヘルパークラス。
    /// REF / LIST / TEXT / NUM / DATE 全て共通。
    /// </summary>
    public static class QualityLogHelper
    {
        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            WriteIndented = false,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        };

        // ========================
        // ① quality_detail_json
        // ========================
        public static string BuildQualityDetail(
            string result,
            string reasonCd,
            string message,
            string sourceRaw,
            string? valueText = null,
            decimal? valueNum = null,
            DateTime? valueDate = null,
            string? valueCd = null)
        {
            var obj = new
            {
                result,
                reason_cds = new[] { reasonCd },
                messages = string.IsNullOrWhiteSpace(message) ? null : new[] { message },
                evidence = new
                {
                    source_raw = sourceRaw,
                    value_text = valueText,
                    value_num = valueNum,
                    value_date = valueDate,
                    value_cd = valueCd
                },
                // sub_objects = new List<object>() // ★ sub_objectsを将来用に追加（今は空）
            };

            return JsonSerializer.Serialize(obj, JsonOptions);
        }

        // ========================
        // ② provenance_json（出処履歴）生成
        // ========================
        public static string BuildProvenance(
            long ruleSetId,
            string ruleVersion,
            long policyId,
            string attrCd,
            string matcherKind,
            int stepNo,
            string sourceRaw,
            string groupCompanyCd,
            string batchId,
            string tempRowId,
            string workerId)
        {
            var provenance = new
            {
                stage = "CLEANSE",
                rule = new
                {
                    rule_set_id = ruleSetId,
                    rule_version = ruleVersion,
                    policy_id = policyId,
                    attr_cd = attrCd,
                    matcher_kind = matcherKind,
                    step_no = stepNo
                },
                input = new
                {
                    source_raw = sourceRaw,
                    context = new
                    {
                        group_company_cd = groupCompanyCd
                    }
                },
                audit = new
                {
                    batch_id = batchId,
                    temp_row_id = tempRowId,
                    run_at = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ"),
                    worker_id = workerId
                }
            };

            return JsonSerializer.Serialize(provenance, JsonOptions);
        }
    }
}
