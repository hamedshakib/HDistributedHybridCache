using HDistributedHybridCache.Abstraction.Contracts;
using HDistributedHybridCache.Abstraction.Models;
using HDistributedHybridCache.Infrastructures;
using HDistributedHybridCache.Infrastructures.Redis;
using HDistributedHybridCache.Infrastructures.Utilities;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Collections.Concurrent;

namespace HDistributedHybridCache.Services;

/// <summary>
/// Main cache service implementation providing hybrid caching (Memory + Redis).
/// </summary>
internal sealed class CacheService : ICacheService, IDisposable
{
    private readonly IMemoryCache _memoryCache;
    private readonly CacheOptions _options;
    private readonly ILogger<CacheService> _logger;
    private readonly HotKeyTracker _hotKeyTracker;
    private readonly CacheStatistics _statistics;
    private readonly ICacheSerializer _serializer;
    private readonly ICacheCompressor? _compressor;
    private readonly RedisConnectionHealthMonitor _redisHealthMonitor;
    private readonly RedisPubSubManager _redisPubSub;
    private readonly RedisPatternDeleter _redisPatternDeleter;
    private readonly RedisKeyHelper _keyHelper;
    private readonly RetryPolicy _retryPolicy;
    private readonly StampedeProtector _stampedeProtector;
    private readonly ConcurrentDictionary<string, Lazy<SemaphoreSlim>> _stampedeLocks;

    // Null Cache Sentinel — a single byte[0] stored in Redis to represent a cached null
    private static readonly byte[] _nullSentinel = [0];

    public CacheService(
        IMemoryCache memoryCache,
        IConnectionMultiplexer redisConnection,
        IOptions<CacheOptions> options,
        ILogger<CacheService> logger,
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
        _stampedeLocks = new ConcurrentDictionary<string, Lazy<SemaphoreSlim>>();

        _stampedeProtector = new StampedeProtector(
            _options.EnableCacheStampedeProtection,
            _options.StampedeLockTimeoutMs,
            _options.StampedeLockCleanupInterval,
            _logger);

        _redisHealthMonitor = new RedisConnectionHealthMonitor(
            redisConnection,
            _options,
            logger,
            _memoryCache,
            _hotKeyTracker,
            _statistics);

        _keyHelper = new RedisKeyHelper(_options);

        _redisPatternDeleter = new RedisPatternDeleter(
            redisConnection.GetDatabase(_options.RedisDatabase),
            logger,
            _options);

        _redisPubSub = new RedisPubSubManager(
            redisConnection,
            _options,
            logger,
            _memoryCache,
            _hotKeyTracker,
            _statistics,
            _stampedeLocks,
            _keyHelper);

        _retryPolicy = new RetryPolicy(_options, logger);
    }

    // --- Direct Redis Get (no L1 Memory caching) ---
    public async Task<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        var redisKey = _keyHelper.GetFullKey(key);
        _statistics.RecordRequest();

        if (!_redisHealthMonitor.IsRedisConnected())
        {
            _statistics.RecordMiss(key);
            return default;
        }

        try
        {
            var redisValue = await _retryPolicy.ExecuteWithRetryAsync(
                () => _redisPatternDeleter.RedisDb.StringGetAsync(redisKey).WaitAsync(cancellationToken),
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

        _redisHealthMonitor.CheckConnectedOrThrow();
        var redisKey = _keyHelper.GetFullKey(cacheKey.Key);

        try
        {
            // Null value → store sentinel if null caching is enabled
            if (value is null && _options.EnableNullCaching)
            {
                await _retryPolicy.ExecuteWithRetryAsync(
                    () => _redisPatternDeleter.RedisDb.StringSetAsync(redisKey, _nullSentinel, _options.NullCacheTtl)
                        .WaitAsync(cancellationToken),
                    cancellationToken).ConfigureAwait(false);

                _logger.LogDebug("Null cache SET: {Key} (TTL: {Ttl})", cacheKey.Key, _options.NullCacheTtl);
                return;
            }

            var data = SerializeValue(value);
            await _retryPolicy.ExecuteWithRetryAsync(
                () => _redisPatternDeleter.RedisDb.StringSetAsync(redisKey, data, cacheKey.PreferredRedisTtl ?? _options.DefaultRedisTtl)
                    .WaitAsync(cancellationToken),
                cancellationToken).ConfigureAwait(false);

            if (ShouldStoreInMemory(cacheKey))
            {
                SetInMemory(cacheKey.Key, value, cacheKey);
            }

            if (_options.EnablePubSub && cacheKey.StoreType != CacheStoreType.NeverInMemory)
            {
                await _redisPubSub.PublishInvalidationAsync(cacheKey.Key).ConfigureAwait(false);
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

        if (!_redisHealthMonitor.IsRedisConnected())
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

        var redisKey = _keyHelper.GetFullKey(cacheKey.Key);

        _memoryCache.Remove(cacheKey.Key);
        _hotKeyTracker.RemoveKey(cacheKey.Key);
        _stampedeProtector.RemoveKey(cacheKey.Key);

        if (_redisHealthMonitor.IsRedisConnected())
        {
            await _retryPolicy.ExecuteWithRetryAsync(
                () => _redisPatternDeleter.RedisDb.KeyDeleteAsync(redisKey).WaitAsync(cancellationToken),
                cancellationToken).ConfigureAwait(false);
        }

        if (_options.EnablePubSub)
        {
            await _redisPubSub.PublishInvalidationAsync(cacheKey.Key).ConfigureAwait(false);
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

    // --- Pattern-based Deletion ---
    public async Task RemoveByPatternAsync(string pattern, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(pattern);

        var redisKeyPattern = _keyHelper.GetFullKeyPattern(pattern);

        long deletedCount = 0;
        if (_redisHealthMonitor.IsRedisConnected())
        {
            deletedCount = await _redisPatternDeleter.RemoveByPatternAsync(redisKeyPattern, true, cancellationToken);

            await _redisPubSub.PublishPatternInvalidationAsync(pattern, cancellationToken);
        }

        ClearMemoryKeysByPattern(pattern);

        _logger.LogInformation("Removed {Count} keys matching pattern '{Pattern}'", deletedCount, pattern);
    }

    // --- Clear Memory keys by pattern (without affecting HotKeys or Statistics) ---
    private void ClearMemoryKeysByPattern(string pattern)
    {
        if (string.IsNullOrEmpty(pattern)) return;

        if (_memoryCache is MemoryCache concreteCache)
        {
            var regexPattern = PatternToRegexConverter.ConvertToRegex(pattern);
            var keysToRemove = concreteCache.Keys
                .Cast<string>()
                .Where(k => System.Text.RegularExpressions.Regex.IsMatch(k, regexPattern))
                .ToList();

            foreach (var key in keysToRemove)
            {
                concreteCache.Remove(key);
            }

            _logger.LogDebug("Cleared {Count} memory keys matching pattern '{Pattern}'", keysToRemove.Count, pattern);
        }
    }

    public CacheStatistics GetStatistics() => _statistics;

    // --- Private Helpers ---

    private bool ShouldStoreInMemory(CacheKey cacheKey)
    {
        if (!_redisHealthMonitor.IsRedisConnected())
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
        var redisKey = _keyHelper.GetFullKey(cacheKey.Key);

        _statistics.RecordRequest();
        _hotKeyTracker.RecordAccess(cacheKey.Key, cacheKey.StoreType == CacheStoreType.HotKeyOnly);

        // L1: Memory Cache
        if (_memoryCache.TryGetValue(cacheKey.Key, out T? memoryValue))
        {
            _statistics.RecordMemoryHit(cacheKey.Key);
            return HDistributedHybridCache.Abstraction.Models.CacheResult<T>.Hit(memoryValue);
        }

        if (!_redisHealthMonitor.IsRedisConnected())
        {
            _statistics.RecordMiss(cacheKey.Key);
            return HDistributedHybridCache.Abstraction.Models.CacheResult<T>.Miss;
        }

        // L2: Redis Cache
        try
        {
            var redisValue = await _retryPolicy.ExecuteWithRetryAsync(
                () => _redisPatternDeleter.RedisDb.StringGetAsync(redisKey).WaitAsync(cancellationToken),
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

                return HDistributedHybridCache.Abstraction.Models.CacheResult<T>.Hit(value);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Redis GET failed for key: {Key}", cacheKey.Key);
        }

        _statistics.RecordMiss(cacheKey.Key);
        return HDistributedHybridCache.Abstraction.Models.CacheResult<T>.Miss;
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

    // --- Dispose ---
    public void Dispose()
    {
        _stampedeProtector.Dispose();
        _redisHealthMonitor.Dispose();
        _redisPubSub.Dispose();
        GC.SuppressFinalize(this);
    }
}