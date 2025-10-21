using System;

namespace ProductDataIngestion.Models
{
    public class MCompany
    {
        [JsonPropertyName("groupCompanyId")]
        public long GroupCompanyId { get; set; }

        [JsonPropertyName("groupCompanyCd")]
        public string GroupCompanyCd { get; set; } = string.Empty;

        [JsonPropertyName("groupCompanyNm")]
        public string? GroupCompanyNm { get; set; }

        [JsonPropertyName("defaultCurrencyCd")]
        public string? DefaultCurrencyCd { get; set; }

        [JsonPropertyName("isActive")]
        public bool IsActive { get; set; } = true;

        [JsonPropertyName("creAt")]
        public DateTime CreAt { get; set; } = DateTime.UtcNow;

        [JsonPropertyName("updAt")]
        public DateTime UpdAt { get; set; } = DateTime.UtcNow;

        public bool IsValid()
        {
            return !string.IsNullOrEmpty(GroupCompanyCd) && 
                   GroupCompanyId > 0;
        }

        public override string ToString()
        {
            return $"MCompany: {GroupCompanyCd} ({GroupCompanyNm}), Currency: {DefaultCurrencyCd}, Active: {IsActive}";
        }
    }
}