using HDistributedHybridCache.Abstraction.Contracts;
using HDistributedHybridCache.Abstraction.Models;
using HDistributedHybridCache.Infrastructures;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Collections.Concurrent;

namespace HDistributedHybridCache.Services;

internal class HDistributedHybridCacheService : ICacheService, IDisposable
{
    private readonly IMemoryCache _memoryCache;
    private readonly CacheOptions _options;
    private readonly ILogger<HDistributedHybridCacheService> _logger;
    private readonly HotKeyTracker _hotKeyTracker;
    private readonly CacheStatistics _statistics;
    private readonly ICacheSerializer _serializer;
    private readonly ICacheCompressor? _compressor;
    private readonly RedisConnectionManager _redis;
    private readonly StampedeProtector _stampedeProtector;

    // Null Cache Sentinel — a single byte[0] stored in Redis to represent a cached null
    private static readonly byte[] _nullSentinel = [0];

    public HDistributedHybridCacheService(
        IMemoryCache memoryCache,
        IConnectionMultiplexer redisConnection,
        IOptions<CacheOptions> options,
        ILogger<HDistributedHybridCacheService> logger,
        ICacheSerializer? serializer = null,
        ICacheCompressor? compressor = null)
    {
        _memoryCache = memoryCache;
        _options = options.Value;
        _logger = logger;

        _hotKeyTracker = new HotKeyTracker(_options);
        _statistics = new CacheStatistics(_options);

        _serializer = serializer ?? new NewtonsoftCacheSerializer();
        _compressor = _options.EnableCompression ? (compressor ?? new GZipCacheCompressor()) : null;

        // Shared stampede locks dictionary for cross-component access
        var stampedeLocks = new ConcurrentDictionary<string, Lazy<SemaphoreSlim>>();

        _stampedeProtector = new StampedeProtector(
            _options.EnableCacheStampedeProtection,
            _options.StampedeLockTimeoutMs,
            _options.StampedeLockCleanupInterval,
            _logger);

        _redis = new RedisConnectionManager(
            redisConnection,
            _options,
            _logger,
            _memoryCache,
            _hotKeyTracker,
            _statistics,
            stampedeLocks);
    }

    // --- Direct Redis Get (no L1 Memory caching) ---
    public async Task<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        var redisKey = _redis.GetFullKey(key);
        _statistics.RecordRequest();

        if (!_redis.IsRedisConnected())
        {
            _statistics.RecordMiss(key);
            return default;
        }

        try
        {
            var redisValue = await ExecuteWithRetryAsync(
                () => _redis.RedisDb.StringGetAsync(redisKey).WaitAsync(cancellationToken),
                cancellationToken).ConfigureAwait(false);

            if (redisValue.HasValue)
            {
                _statistics.RecordRedisHit(key);
                return DeserializeData<T>((byte[])redisValue!, key);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Redis GET failed for key: {Key}", key);
        }

        _statistics.RecordMiss(key);
        return default;
    }

    // --- Set with CacheKey (full L1/L2 strategy) ---
    public async Task SetAsync<T>(CacheKey cacheKey, T value, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(cacheKey);

        _redis.CheckConnectionOrThrow();
        var redisKey = _redis.GetFullKey(cacheKey.Key);

        try
        {
            // Null value → store sentinel if null caching is enabled
            if (value is null && _options.EnableNullCaching)
            {
                await ExecuteWithRetryAsync(
                    () => _redis.RedisDb.StringSetAsync(redisKey, _nullSentinel, _options.NullCacheTtl)
                        .WaitAsync(cancellationToken),
                    cancellationToken).ConfigureAwait(false);

                _logger.LogDebug("Null cache SET: {Key} (TTL: {Ttl})", cacheKey.Key, _options.NullCacheTtl);
                return;
            }

            var data = SerializeValue(value);
            await ExecuteWithRetryAsync(
                () => _redis.RedisDb.StringSetAsync(redisKey, data, cacheKey.PreferredRedisTtl ?? _options.DefaultRedisTtl)
                    .WaitAsync(cancellationToken),
                cancellationToken).ConfigureAwait(false);

            if (ShouldStoreInMemory(cacheKey))
            {
                SetInMemory(cacheKey.Key, value, cacheKey);
            }

            if (_options.EnablePubSub && cacheKey.StoreType != CacheStoreType.NeverInMemory)
            {
                await _redis.PublishInvalidationAsync(cacheKey.Key).ConfigureAwait(false);
            }

            _logger.LogDebug("Cache SET: {Key} (StoreType: {StoreType})", cacheKey.Key, cacheKey.StoreType);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Redis SET failed for key: {Key}", cacheKey.Key);
            throw;
        }
    }

    // --- GetOrSet with CacheKey (with stampede protection) ---
    public async Task<T> GetOrSetAsync<T>(
        CacheKey cacheKey,
        Func<CancellationToken, Task<T>> factory,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(cacheKey);
        ArgumentNullException.ThrowIfNull(factory);

        // First try cache (L1 + L2) — CacheResult.HasValue distinguishes null-hit from miss
        var cacheResult = await GetFromCacheAsync<T>(cacheKey, cancellationToken).ConfigureAwait(false);
        if (cacheResult.HasValue)
            return cacheResult.Value!;

        if (!_redis.IsRedisConnected())
        {
            _logger.LogWarning("Redis is disconnected. GetOrSetAsync will use factory directly for key: {Key}", cacheKey.Key);
            return await factory(cancellationToken).ConfigureAwait(false);
        }

        if (_options.EnableCacheStampedeProtection)
        {
            return await _stampedeProtector.ExecuteAsync(
                cacheKey.Key,
                ct => GetFromCacheAsync<T>(cacheKey, ct),
                factory,
                cancellationToken,
                (value, ct) => SetAsync(cacheKey, value, ct)).ConfigureAwait(false);
        }

        var value = await factory(cancellationToken).ConfigureAwait(false);
        await SetAsync(cacheKey, value, cancellationToken).ConfigureAwait(false);
        return value;
    }

    // --- Remove ---
    public async Task RemoveAsync(CacheKey cacheKey, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(cacheKey);

        var redisKey = _redis.GetFullKey(cacheKey.Key);

        _memoryCache.Remove(cacheKey.Key);
        _hotKeyTracker.RemoveKey(cacheKey.Key);
        _stampedeProtector.RemoveKey(cacheKey.Key);

        if (_redis.IsRedisConnected())
        {
            await _redis.RedisDb.KeyDeleteAsync(redisKey).WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        if (_options.EnablePubSub)
        {
            await _redis.PublishInvalidationAsync(cacheKey.Key).ConfigureAwait(false);
        }
    }

    // --- Management ---
    public void ClearMemoryCache()
    {
        if (_memoryCache is MemoryCache concreteCache)
        {
            concreteCache.Compact(1.0);
            _logger.LogInformation("Memory cache cleared (Compact 100%).");
        }
        else
        {
            _logger.LogWarning("Cannot clear memory cache: IMemoryCache is not a MemoryCache instance.");
        }
        _hotKeyTracker.Cleanup();
    }

    public CacheStatistics GetStatistics() => _statistics;

    // --- Private Helpers ---

    private bool ShouldStoreInMemory(CacheKey cacheKey)
    {
        if (!_redis.IsRedisConnected())
            return false;

        return cacheKey.StoreType switch
        {
            CacheStoreType.NeverInMemory => false,
            CacheStoreType.HotKeyOnly => _hotKeyTracker.IsHotKey(cacheKey.Key),
            CacheStoreType.PreferInMemory => true,
            CacheStoreType.MustInMemory => true,
            _ => false
        };
    }

    // Core cache lookup — checks L1 (Memory) then L2 (Redis)
    private async Task<CacheResult<T>> GetFromCacheAsync<T>(CacheKey cacheKey, CancellationToken cancellationToken)
    {
        var redisKey = _redis.GetFullKey(cacheKey.Key);

        _statistics.RecordRequest();
        _hotKeyTracker.RecordAccess(cacheKey.Key, cacheKey.StoreType == CacheStoreType.HotKeyOnly);

        // L1: Memory Cache
        if (_memoryCache.TryGetValue(cacheKey.Key, out T? memoryValue))
        {
            _statistics.RecordMemoryHit(cacheKey.Key);
            return CacheResult<T>.Hit(memoryValue);
        }

        if (!_redis.IsRedisConnected())
        {
            _statistics.RecordMiss(cacheKey.Key);
            return CacheResult<T>.Miss;
        }

        // L2: Redis Cache
        try
        {
            var redisValue = await ExecuteWithRetryAsync(
                () => _redis.RedisDb.StringGetAsync(redisKey).WaitAsync(cancellationToken),
                cancellationToken).ConfigureAwait(false);

            if (redisValue.HasValue)
            {
                _statistics.RecordRedisHit(cacheKey.Key);
                var value = DeserializeData<T>((byte[])redisValue!, cacheKey.Key);

                // Store non-null values in memory cache if applicable
                if (value is not null && ShouldStoreInMemory(cacheKey))
                {
                    SetInMemory(cacheKey.Key, value, cacheKey);
                }

                return CacheResult<T>.Hit(value);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Redis GET failed for key: {Key}", cacheKey.Key);
        }

        _statistics.RecordMiss(cacheKey.Key);
        return CacheResult<T>.Miss;
    }

    private T? DeserializeData<T>(byte[] data, string logKey)
    {
        if (_options.EnableNullCaching && data.Length == 1 && data[0] == 0)
        {
            _logger.LogDebug("Null cache HIT for key: {Key}", logKey);
            return default;
        }

        if (_compressor is not null)
            data = _compressor.Decompress(data);

        return _serializer.Deserialize<T>(data);
    }

    private byte[] SerializeValue<T>(T value)
    {
        var data = _serializer.Serialize(value);
        return _compressor is not null ? _compressor.Compress(data) : data;
    }

    private void SetInMemory<T>(string key, T value, CacheKey cacheKey)
    {
        var redisTtl = cacheKey.PreferredRedisTtl ?? _options.DefaultRedisTtl;
        var memoryTtl = cacheKey.PreferredMemoryTtl;

        var ttl = memoryTtl.HasValue
            ? (memoryTtl.Value > redisTtl ? redisTtl : memoryTtl.Value)
            : redisTtl;

        var priority = GetMemoryPriority(cacheKey.StoreType);

        var entryOptions = new MemoryCacheEntryOptions()
            .SetPriority(priority)
            .SetSize(1);

        if (ttl != TimeSpan.MaxValue && ttl != TimeSpan.Zero)
        {
            // Combine Absolute + Sliding:
            // - Absolute: max lifetime in memory
            // - Sliding: expire if not accessed within 1 min
            entryOptions.SetAbsoluteExpiration(ttl);
            entryOptions.SetSlidingExpiration(TimeSpan.FromMinutes(1));
        }

        var storeType = cacheKey.StoreType;

        entryOptions.RegisterPostEvictionCallback((evictedKey, _, reason, _) =>
        {
            _logger.LogDebug("Memory evicted: {Key}, Reason: {Reason}", evictedKey, reason);
            if (reason == EvictionReason.Capacity || reason == EvictionReason.Expired)
            {
                _hotKeyTracker.RemoveKey(evictedKey?.ToString() ?? "");
            }

            if (storeType == CacheStoreType.MustInMemory && reason == EvictionReason.Capacity)
            {
                _logger.LogWarning("🚨 MUST_IN_MEMORY key evicted: {Key}", key);
            }
        });

        _memoryCache.Set(key, value, entryOptions);
    }

    private static CacheItemPriority GetMemoryPriority(CacheStoreType storeType) => storeType switch
    {
        CacheStoreType.PreferInMemory => CacheItemPriority.High,
        CacheStoreType.MustInMemory => CacheItemPriority.NeverRemove,
        _ => CacheItemPriority.Normal
    };

    // --- Retry Policy with Exponential Backoff ---
    private async Task<T> ExecuteWithRetryAsync<T>(Func<Task<T>> operation, CancellationToken cancellationToken)
    {
        var retryCount = _options.RedisRetryCount;
        var baseDelay = _options.RedisRetryBaseDelayMs;

        Exception? lastException = null;

        for (int attempt = 0; attempt <= retryCount; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                return await operation().ConfigureAwait(false);
            }
            catch (Exception ex) when (attempt < retryCount && ex is not OperationCanceledException)
            {
                lastException = ex;
                _logger.LogWarning(ex,
                    "Redis operation failed (attempt {Attempt}/{MaxRetries}). Retrying...",
                    attempt + 1, retryCount);

                var delay = TimeSpan.FromMilliseconds(baseDelay * Math.Pow(2, attempt));
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }

        throw new RedisException($"Redis operation failed after {retryCount + 1} attempts", lastException);
    }

    // --- Dispose ---
    public void Dispose()
    {
        _stampedeProtector.Dispose();
        _redis.Dispose();
        GC.SuppressFinalize(this);
    }
}