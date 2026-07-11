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

    private string GetInvalidationChannel() => $"{_options.PubSubChannelPrefix}:invalidate:key";

    private string GetPatternInvalidationChannel() => $"{_options.PubSubChannelPrefix}:invalidate:pattern";

    private void SubscribeToInvalidationEvents()
    {
        if (_redisSubscriber == null) return;

        if (_subscribed)
        {
            var oldChannel = new RedisChannel(GetInvalidationChannel(), RedisChannel.PatternMode.Literal);
            _redisSubscriber.UnsubscribeAsync(oldChannel).Wait();

            var oldPatternChannel = new RedisChannel(GetPatternInvalidationChannel(), RedisChannel.PatternMode.Literal);
            _redisSubscriber.UnsubscribeAsync(oldPatternChannel).Wait();
        }

        // کانال برای کلید تکی
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

        // کانال برای pattern
        var patternChannel = new RedisChannel(GetPatternInvalidationChannel(), RedisChannel.PatternMode.Literal);
        _redisSubscriber.SubscribeAsync(patternChannel, async (redisChannel, message) =>
        {
            try
            {
                var pattern = message.ToString();
                if (string.IsNullOrEmpty(pattern)) return;

                // فقط Memory keys حذف می‌شوند (HotKeys و آمار نگه داشته می‌شوند)
                ClearMemoryByPattern(pattern);

                // فقط Stampede locks حذف می‌شوند
                RemoveStampedeLocksByPattern(pattern);

                _logger.LogDebug("🔄 Cleared memory keys matching pattern (HotKeys preserved): {Pattern}", pattern);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Pub/Sub pattern error");
            }
        });

        _subscribed = true;
    }

    // --- Clear Memory keys by pattern (without affecting HotKeys or Statistics) ---
    private void ClearMemoryByPattern(string pattern)
    {
        if (string.IsNullOrEmpty(pattern)) return;

        if (_memoryCache is MemoryCache concreteCache)
        {
            var regexPattern = PatternToRegex(pattern);
            var keysToRemove = concreteCache.Keys
                .Cast<string>()
                .Where(k => System.Text.RegularExpressions.Regex.IsMatch(k, regexPattern))
                .ToList();

            foreach (var key in keysToRemove)
            {
                concreteCache.Remove(key);
            }

            _logger.LogDebug("🔄 Cleared {Count} memory keys matching pattern '{Pattern}'", keysToRemove.Count, pattern);
        }
    }

    private void RemoveStampedeLocksByPattern(string pattern)
    {
        if (string.IsNullOrEmpty(pattern)) return;

        var regexPattern = PatternToRegex(pattern);
        var keysToRemove = _stampedeLocks.Keys
            .Where(k => System.Text.RegularExpressions.Regex.IsMatch(k, regexPattern))
            .ToList();

        foreach (var key in keysToRemove)
        {
            _stampedeLocks.TryRemove(key, out _);
        }
    }

    private static string PatternToRegex(string pattern)
    {
        var regexPattern = System.Text.RegularExpressions.Regex.Escape(pattern)
            .Replace("\\*", ".*")
            .Replace("\\?", ".");
        return "^" + regexPattern + "$";
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
    // Pattern-based Deletion
    // ================================================================

    /// <summary>
    /// Removes all keys matching the specified pattern from Redis using SCAN.
    /// Returns the number of keys deleted.
    /// </summary>
    public async Task<long> RemoveByPatternAsync(string pattern, CancellationToken cancellationToken = default)
    {
        if (!_redisConnected) return 0;

        long deletedCount = 0;
        var cursor = 0L;

        // SCAN with COUNT 1000 for batch processing
        const int batchSize = 1000;

        do
        {
            // Use Lua script to scan and delete in one operation
            var script = @"
                local cursor = tonumber(ARGV[1])
                local count = tonumber(ARGV[2])
                local result = redis.call('SCAN', cursor, 'MATCH', KEYS[1], 'COUNT', count)

                local newCursor = tonumber(result[1])
                local keys = result[2]

                if #keys > 0 then
                    redis.call('DEL', unpack(keys))
                end

                return {tostring(newCursor), #keys}
            ";

            var result = await _redisDb.ScriptEvaluateAsync(
                script,
                new RedisKey[] { pattern },
                new RedisValue[] { cursor, batchSize }
            ).WaitAsync(cancellationToken);

            // Parse result
            var resultString = result.ToString();
            var parts = resultString.Split(' ');
            
            if (parts.Length >= 2)
            {
                cursor = long.Parse(parts[0]);
                deletedCount += int.Parse(parts[1]);
            }
            else
            {
                cursor = 0;
            }

        } while (cursor != 0);

        return deletedCount;
    }

    /// <summary>
    /// Publishes pattern invalidation to other nodes via Pub/Sub.
    /// Other nodes will remove keys matching this pattern from their memory cache.
    /// </summary>
    public async Task PublishPatternInvalidationAsync(string pattern, CancellationToken cancellationToken = default)
    {
        if (_redisSubscriber == null || !_redisConnected) return;

        try
        {
            var channel = new RedisChannel(GetPatternInvalidationChannel(), RedisChannel.PatternMode.Literal);
            // Publish pattern to other nodes so they can clean their memory cache
            await _redisSubscriber.PublishAsync(channel, pattern).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish pattern invalidation: {Pattern}", pattern);
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