using HDistributedHybridCache.Abstraction.Models;

namespace HDistributedHybridCache.Abstraction.Contracts;

public interface ICacheService
{
    // ============ With string (Redis only, no L1 Memory caching) ============
    Task<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default);

    // ============ With CacheKey (full storage strategy) ============
    Task SetAsync<T>(CacheKey cacheKey, T value, CancellationToken cancellationToken = default);
    Task RemoveAsync(CacheKey cacheKey, CancellationToken cancellationToken = default);
    Task<T> GetOrSetAsync<T>(
        CacheKey cacheKey,
        Func<CancellationToken, Task<T>> factory,
        CancellationToken cancellationToken = default
    );
    Task RemoveByPatternAsync(string pattern, CancellationToken cancellationToken = default);

    // ============ Management ============
    void ClearMemoryCache();
    CacheStatistics GetStatistics();
}