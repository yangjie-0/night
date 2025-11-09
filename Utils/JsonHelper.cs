using System.Text.Json;

namespace ProductDataIngestion.Utils
{
    public static class JsonHelper
    {
        private static readonly JsonSerializerOptions _options = new()
        {
            WriteIndented = false,
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };

        /// <summary>
        /// 安全にJSONにシリアライズする。
        /// </summary>
        public static string SafeSerialize(object? obj)
        {
            try
            {
                if (obj == null) return "{}";
                return JsonSerializer.Serialize(obj, _options);
            }
            catch
            {
                return "{}";
            }
        }
    }
}
