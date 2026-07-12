using HDistributedHybridCache.Abstraction.Models;
using HDistributedHybridCache.Services;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace HDistributedHybridCache.Infrastructures.Redis;

/// <summary>
/// Monitors Redis connection health and handles connection events.
/// </summary>
internal sealed class RedisConnectionHealthMonitor : IDisposable
{
    private readonly IConnectionMultiplexer _redisConnection;
    private readonly CacheOptions _options;
    private readonly ILogger _logger;
    private readonly IMemoryCache _memoryCache;
    private readonly HotKeyTracker _hotKeyTracker;
    private readonly CacheStatistics _statistics;

    private volatile bool _redisConnected;

    public RedisConnectionHealthMonitor(
        IConnectionMultiplexer redisConnection,
        CacheOptions options,
        ILogger logger,
        IMemoryCache memoryCache,
        HotKeyTracker hotKeyTracker,
        CacheStatistics statistics)
    {
        _redisConnection = redisConnection;
        _options = options;
        _logger = logger;
        _memoryCache = memoryCache;
        _hotKeyTracker = hotKeyTracker;
        _statistics = statistics;

        _redisConnected = redisConnection.IsConnected;

        redisConnection.ConnectionFailed += OnConnectionFailed;
        redisConnection.ConnectionRestored += OnConnectionRestored;
    }

    public void CheckConnectedOrThrow()
    {
        if (!_redisConnected)
        {
            throw new RedisConnectionException(ConnectionFailureType.UnableToConnect,
                "Cannot use cache: Redis is disconnected.");
        }
    }

    public bool IsRedisConnected()
    {
        if (!_redisConnected)
        {
            _logger.LogWarning("Redis is disconnected. Skipping Redis operations.");
            return false;
        }
        return true;
    }

    private void OnConnectionFailed(object? sender, ConnectionFailedEventArgs e)
    {
        _redisConnected = false;
        _logger.LogError(e.Exception, "🔴 Redis connection FAILED. Clearing memory cache to prevent data inconsistency.");

        if (_memoryCache is MemoryCache concreteCache)
        {
            concreteCache.Compact(1.0);
        }
        _hotKeyTracker.Cleanup();
        _statistics.Reset();
    }

    private async void OnConnectionRestored(object? sender, ConnectionFailedEventArgs e)
    {
        _redisConnected = true;
        _logger.LogInformation("🟢 Redis connection RESTORED.");

        // Note: Pub/Sub re-subscription is handled by RedisPubSubManager
    }

    public void Dispose()
    {
        _redisConnection.ConnectionFailed -= OnConnectionFailed;
        _redisConnection.ConnectionRestored -= OnConnectionRestored;
    }
}