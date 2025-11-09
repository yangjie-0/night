using System.Text.Json;

namespace ProductDataIngestion.Utils
{
    public static class ProvenanceHelper
    {
        public static string AppendProvenanceJson(string? existingJson, object newEntry)
        {
            // ① 当前为空 → 新建数组
            if (string.IsNullOrWhiteSpace(existingJson))
                return JsonSerializer.Serialize(new List<object> { newEntry });

            try
            {
                // ② 如果原本就是数组，则追加
                var list = JsonSerializer.Deserialize<List<object>>(existingJson);
                if (list != null)
                {
                    list.Add(newEntry);
                    return JsonSerializer.Serialize(list);
                }
            }
            catch
            {
                // ③ 如果原本是单个对象，则包一层数组再追加
                try
                {
                    var single = JsonSerializer.Deserialize<object>(existingJson);
                    return JsonSerializer.Serialize(new List<object> { single!, newEntry });
                }
                catch
                {
                    // 万一都解析失败 → fallback
                    return JsonSerializer.Serialize(new List<object> { newEntry });
                }
            }

            // ④ 兜底：始终返回数组
            return JsonSerializer.Serialize(new List<object> { newEntry });
        }
    }
}
