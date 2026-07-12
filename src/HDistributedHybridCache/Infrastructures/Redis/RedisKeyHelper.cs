using HDistributedHybridCache.Abstraction.Models;

namespace HDistributedHybridCache.Infrastructures.Redis;

/// <summary>
/// Helper class for Redis key operations including prefix handling.
/// </summary>
internal sealed class RedisKeyHelper
{
    private readonly string _keyPrefix;

    public RedisKeyHelper(CacheOptions options)
    {
        _keyPrefix = options.KeyPrefix;
    }

    public string GetFullKey(string key) =>
        string.IsNullOrEmpty(_keyPrefix) ? key : $"{_keyPrefix}:{key}";

    public string GetFullKeyPattern(string pattern) =>
        string.IsNullOrEmpty(_keyPrefix) ? pattern : $"{_keyPrefix}:{pattern}";
}