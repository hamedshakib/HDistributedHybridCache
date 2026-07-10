using System.Text;
using HDistributedHybridCache.Abstraction.Contracts;
using Newtonsoft.Json;

namespace HDistributedHybridCache.Services;

/// <summary>
/// پیاده‌سازی پیش‌فرض ICacheSerializer با Newtonsoft.Json
/// </summary>
public class NewtonsoftCacheSerializer : ICacheSerializer
{
    private readonly JsonSerializerSettings _settings;

    public NewtonsoftCacheSerializer()
    {
        _settings = new JsonSerializerSettings
        {
            ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
            Formatting = Formatting.None // بدون indentation برای کاهش حجم
        };
    }

    public byte[] Serialize<T>(T value)
    {
        var json = JsonConvert.SerializeObject(value, _settings);
        return Encoding.UTF8.GetBytes(json);
    }

    public T? Deserialize<T>(byte[] data)
    {
        if (data == null || data.Length == 0)
            return default;

        var json = Encoding.UTF8.GetString(data);
        return JsonConvert.DeserializeObject<T>(json, _settings);
    }
}