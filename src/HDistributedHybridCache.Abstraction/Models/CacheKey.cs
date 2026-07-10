namespace HDistributedHybridCache.Abstraction.Models;

public class CacheKey
{
    public string Key { get; }
    public CacheStoreType StoreType { get; }
    public TimeSpan? PreferredRedisTtl { get; }
    public TimeSpan? PreferredMemoryTtl { get; }

    private CacheKey(
        string key,
        CacheStoreType storeType,
        TimeSpan? preferredRedisTtl,
        TimeSpan? preferredMemoryTtl)
    {
        Key = key;
        StoreType = storeType;
        PreferredRedisTtl = preferredRedisTtl;
        PreferredMemoryTtl = storeType == CacheStoreType.NeverInMemory ? null : preferredMemoryTtl;
    }

    // Helper methods for easier construction
    public static CacheKey NeverInMemory(string key, TimeSpan? redisTtl = null)
        => new(key, CacheStoreType.NeverInMemory, redisTtl, null);

    public static CacheKey HotKeyOnly(string key, TimeSpan? redisTtl = null, TimeSpan? memoryTtl = null)
        => new(key, CacheStoreType.HotKeyOnly, redisTtl, memoryTtl);

    public static CacheKey PreferInMemory(string key, TimeSpan? redisTtl = null, TimeSpan? memoryTtl = null)
        => new(key, CacheStoreType.PreferInMemory, redisTtl, memoryTtl);

    public static CacheKey MustInMemory(string key, TimeSpan? redisTtl = null, TimeSpan? memoryTtl = null)
        => new(key, CacheStoreType.MustInMemory, redisTtl, memoryTtl);
}