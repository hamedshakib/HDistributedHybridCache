using HDistributedHybridCache.Abstraction.Models;

namespace HDistributedHybridCache.Abstraction.Contracts;

public interface ICacheService
{
    // ============ با string (فقط Redis خوانده می‌شود، بدون L1 Memory) ============
    Task<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default);

    // ============ با CacheKey (کامل با استراتژی ذخیره‌سازی) ============
    Task SetAsync<T>(CacheKey cacheKey, T value, CancellationToken cancellationToken = default);
    Task RemoveAsync(CacheKey cacheKey, CancellationToken cancellationToken = default);
    Task<T> GetOrSetAsync<T>(
        CacheKey cacheKey,
        Func<CancellationToken, Task<T>> factory,
        CancellationToken cancellationToken = default
    );

    // ============ مدیریت ============
    void ClearMemoryCache();
    CacheStatistics GetStatistics();
}