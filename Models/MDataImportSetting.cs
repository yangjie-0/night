using System;
using System.Text.Json.Serialization;

namespace ProductDataIngestion.Models
{
    public class MDataImportSetting
    {
        public long ProfileId { get; set; }

        public string UsageNm { get; set; } = string.Empty;

        public string GroupCompanyCd { get; set; } = string.Empty;

        public string TargetEntity { get; set; } = string.Empty;

        public string CharacterCd { get; set; } = string.Empty;

        public string Delimiter { get; set; } = string.Empty;

        public int HeaderRowIndex { get; set; } = 1;  // 默认头行为第1行

        public int SkipRowCount { get; set; } = 0;    // 显式默认0，避免误用

        public bool IsActive { get; set; }

        public string ImportSettingRemarks { get; set; } = string.Empty;

        public DateTime CreAt { get; set; }

        public DateTime UpdAt { get; set; }

        [JsonIgnore]
        public string SkipRows => SkipRowCount.ToString();

        public bool IsValid()
        {
            return !string.IsNullOrEmpty(CharacterCd) &&
                   !string.IsNullOrEmpty(Delimiter) &&
                   HeaderRowIndex >= 0 &&
                   SkipRowCount >= 0;
        }

        public override string ToString()
        {
            return $"ProfileId: {ProfileId}, Usage: {UsageNm}, Target: {TargetEntity}, " +
                   $"Encoding: {CharacterCd}, Delimiter: '{Delimiter}', " +
                   $"HeaderRow: {HeaderRowIndex}, SkipRows: {SkipRowCount}";
        }
    }
}