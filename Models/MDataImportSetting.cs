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
        public int HeaderRowIndex { get; set; }  // 注意：数据库是 INT 类型
        public int SkipRowCount { get; set; }    // 注意：数据库是 INT 类型
        public bool IsActive { get; set; }
        public string ImportSettingRemarks { get; set; } = string.Empty;
        public DateTime CreAt { get; set; }
        public DateTime UpdAt { get; set; }

        // 添加计算属性用于兼容性
        [JsonIgnore]
        public string SkipRows => SkipRowCount.ToString();

        // 添加验证方法
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