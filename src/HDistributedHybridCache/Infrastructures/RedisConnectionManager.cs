using HDistributedHybridCache.Abstraction.Models;
using HDistributedHybridCache.Services;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System.Collections.Concurrent;

namespace HDistributedHybridCache.Infrastructures;

/// <summary>
/// Manages Redis connection health monitoring and Pub/Sub invalidation events.
/// </summary>
internal sealed class RedisConnectionManager : IDisposable
{
    private readonly IConnectionMultiplexer _redisConnection;
    private readonly IDatabase _redisDb;
    private readonly ISubscriber? _redisSubscriber;
    private readonly CacheOptions _options;
    private readonly ILogger _logger;
    private readonly IMemoryCache _memoryCache;
    private readonly HotKeyTracker _hotKeyTracker;
    private readonly CacheStatistics _statistics;
    private readonly ConcurrentDictionary<string, Lazy<SemaphoreSlim>> _stampedeLocks;

    private volatile bool _redisConnected;
    private volatile bool _subscribed;

    public bool IsConnected => _redisConnected;
    public IDatabase RedisDb => _redisDb;

    public RedisConnectionManager(
        IConnectionMultiplexer redisConnection,
        CacheOptions options,
        ILogger logger,
        IMemoryCache memoryCache,
        HotKeyTracker hotKeyTracker,
        CacheStatistics statistics,
        ConcurrentDictionary<string, Lazy<SemaphoreSlim>> stampedeLocks)
    {
        _redisConnection = redisConnection;
        _redisDb = redisConnection.GetDatabase(options.RedisDatabase);
        _options = options;
        _logger = logger;
        _memoryCache = memoryCache;
        _hotKeyTracker = hotKeyTracker;
        _statistics = statistics;
        _stampedeLocks = stampedeLocks;

        _redisConnected = redisConnection.IsConnected;

        if (_options.EnablePubSub)
        {
            _redisSubscriber = redisConnection.GetSubscriber();
            SubscribeToInvalidationEvents();
        }

        redisConnection.ConnectionFailed += OnConnectionFailed;
        redisConnection.ConnectionRestored += OnConnectionRestored;
    }

    public void CheckConnectionOrThrow()
    {
        if (!_redisConnected)
        {
            throw new RedisConnectionException(ConnectionFailureType.UnableToConnect,
                "Cannot write to cache: Redis is disconnected.");
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

    // ================================================================
    // Redis Connection Events
    // ================================================================

    private void OnConnectionFailed(object? sender, ConnectionFailedEventArgs e)
    {
        _redisConnected = false;
        _subscribed = false;
        _logger.LogError(e.Exception, "🔴 Redis connection FAILED. Clearing memory cache to prevent data inconsistency.");

        if (_memoryCache is MemoryCache concreteCache)
        {
            concreteCache.Compact(1.0);
        }
        _hotKeyTracker.Cleanup();
        _statistics.Reset();
    }

    private void OnConnectionRestored(object? sender, ConnectionFailedEventArgs e)
    {
        _redisConnected = true;
        _logger.LogInformation("🟢 Redis connection RESTORED. Re-subscribing to invalidation events...");

        if (_options.EnablePubSub)
        {
            SubscribeToInvalidationEvents();
        }
    }

    // ================================================================
    // Pub/Sub Invalidation
    // ================================================================

    private string GetInvalidationChannel() =>
        string.IsNullOrEmpty(_options.KeyPrefix)
            ? _options.PubSubChannelPrefix
            : $"{_options.PubSubChannelPrefix}:{_options.KeyPrefix}";

    private void SubscribeToInvalidationEvents()
    {
        if (_redisSubscriber == null) return;

        if (_subscribed)
        {
            var oldChannel = new RedisChannel(GetInvalidationChannel(), RedisChannel.PatternMode.Literal);
            _redisSubscriber.UnsubscribeAsync(oldChannel);
        }

        var channel = new RedisChannel(GetInvalidationChannel(), RedisChannel.PatternMode.Literal);
        _redisSubscriber.SubscribeAsync(channel, async (redisChannel, message) =>
        {
            try
            {
                var key = message.ToString();
                if (string.IsNullOrEmpty(key)) return;

                _memoryCache.Remove(key);
                _hotKeyTracker.RemoveKey(key);
                _stampedeLocks.TryRemove(key, out _);
                _statistics.RecordInvalidation();

                _logger.LogDebug("🔄 Memory invalidated via Pub/Sub: {Key}", key);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Pub/Sub error");
            }
        });

        _subscribed = true;
    }

    public async Task PublishInvalidationAsync(string key)
    {
        if (_redisSubscriber == null || !_redisConnected) return;

        try
        {
            var channel = new RedisChannel(GetInvalidationChannel(), RedisChannel.PatternMode.Literal);
            await _redisSubscriber.PublishAsync(channel, key).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish invalidation: {Key}", key);
        }
    }

    // ================================================================
    // Key Helpers
    // ================================================================

    public string GetFullKey(string key) =>
        string.IsNullOrEmpty(_options.KeyPrefix) ? key : $"{_options.KeyPrefix}:{key}";

    public void Dispose()
    {
        _redisConnection.ConnectionFailed -= OnConnectionFailed;
        _redisConnection.ConnectionRestored -= OnConnectionRestored;
    }
}