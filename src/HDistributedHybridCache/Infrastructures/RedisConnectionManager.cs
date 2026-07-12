using HDistributedHybridCache.Abstraction.Models;
using HDistributedHybridCache.Services;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System.Collections.Concurrent;
using System.Text.RegularExpressions;

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

    private readonly string _instanceId = Guid.NewGuid().ToString("N");
    public string InstanceId => _instanceId;
    private const int InstanceIdLength = 32;

    // Cache of compiled regexes per pattern to avoid re-building/re-compiling
    // a Regex object on every single invalidation message.
    private readonly ConcurrentDictionary<string, Regex> _patternRegexCache = new();

    private volatile bool _redisConnected;
    private volatile bool _subscribed;

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
            // Fire-and-forget on the constructor; SubscribeAsync itself is async internally,
            // but we block here once at startup so the manager is fully ready when returned.
            SubscribeToInvalidationEventsAsync().GetAwaiter().GetResult();
        }

        redisConnection.ConnectionFailed += OnConnectionFailed;
        redisConnection.ConnectionRestored += OnConnectionRestored;
    }

    public void CheckConnectionOrThrow()
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

    private async void OnConnectionRestored(object? sender, ConnectionFailedEventArgs e)
    {
        _redisConnected = true;
        _logger.LogInformation("🟢 Redis connection RESTORED. Re-subscribing to invalidation events...");

        if (_options.EnablePubSub)
        {
            try
            {
                await SubscribeToInvalidationEventsAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to re-subscribe to invalidation events after reconnect.");
            }
        }
    }

    // ================================================================
    // Pub/Sub Invalidation
    // ================================================================

    private string GetInvalidationChannel() => $"{_options.PubSubChannelPrefix}:invalidate:key";

    private string GetPatternInvalidationChannel() => $"{_options.PubSubChannelPrefix}:invalidate:pattern";

    private async Task SubscribeToInvalidationEventsAsync()
    {
        if (_redisSubscriber == null) return;

        await _redisSubscriber.UnsubscribeAllAsync().ConfigureAwait(false);

        // کانال برای کلید تکی
        var channel = new RedisChannel(GetInvalidationChannel(), RedisChannel.PatternMode.Literal);
        await _redisSubscriber.SubscribeAsync(channel, (redisChannel, message) =>
        {
            try
            {
                var raw = message.ToString();
                if (string.IsNullOrEmpty(raw) || raw.Length <= InstanceIdLength) return;

                var senderInstanceId = raw[..InstanceIdLength];
                var key = raw[InstanceIdLength..];

                if (senderInstanceId == _instanceId)
                {
                    // این پیام از خود همین instance است. چون مقدار جدید همین الان
                    // در SetAsync، قبل از Publish، در مموری لوکال ست شده،
                    // نیازی به حذف/invalidate نیست.
                    _logger.LogDebug("🔁 Self-originated update skipped: {Key}", key);
                    return;
                }

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
        }).ConfigureAwait(false);

        // کانال برای pattern
        var patternChannel = new RedisChannel(GetPatternInvalidationChannel(), RedisChannel.PatternMode.Literal);
        await _redisSubscriber.SubscribeAsync(patternChannel, (redisChannel, message) =>
        {
            try
            {
                var raw = message.ToString();
                if (string.IsNullOrEmpty(raw) || raw.Length <= InstanceIdLength) return;

                var senderInstanceId = raw[..InstanceIdLength];
                var pattern = raw[InstanceIdLength..];

                if (senderInstanceId == _instanceId)
                {
                    _logger.LogDebug("🔁 Self-originated pattern update skipped: {Pattern}", pattern);
                    return;
                }

                if (string.IsNullOrEmpty(pattern)) return;

                ClearMemoryByPattern(pattern);
                RemoveStampedeLocksByPattern(pattern);

                _logger.LogDebug("🔄 Cleared memory keys matching pattern (HotKeys preserved): {Pattern}", pattern);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Pub/Sub pattern error");
            }
        }).ConfigureAwait(false);

        _subscribed = true;
    }

    // --- Clear Memory keys by pattern (without affecting HotKeys or Statistics) ---
    private void ClearMemoryByPattern(string pattern)
    {
        if (string.IsNullOrEmpty(pattern)) return;

        if (_memoryCache is MemoryCache concreteCache)
        {
            var regex = GetOrCreatePatternRegex(pattern);
            var keysToRemove = concreteCache.Keys
                .Cast<string>()
                .Where(k => regex.IsMatch(k))
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

        var regex = GetOrCreatePatternRegex(pattern);
        var keysToRemove = _stampedeLocks.Keys
            .Where(k => regex.IsMatch(k))
            .ToList();

        foreach (var key in keysToRemove)
        {
            _stampedeLocks.TryRemove(key, out _);
        }
    }

    private Regex GetOrCreatePatternRegex(string pattern)
    {
        return _patternRegexCache.GetOrAdd(pattern, static p =>
            new Regex(PatternToRegex(p), RegexOptions.Compiled));
    }

    /// <summary>
    /// Converts a Redis glob-style pattern (as used by SCAN/KEYS MATCH) into an
    /// equivalent .NET regex. Supports '*', '?', and character classes like
    /// '[abc]', '[^abc]', '[a-z]', as well as backslash-escaped literals.
    /// </summary>
    private static string PatternToRegex(string pattern)
    {
        var sb = new System.Text.StringBuilder("^");
        var i = 0;
        while (i < pattern.Length)
        {
            var c = pattern[i];
            switch (c)
            {
                case '*':
                    sb.Append(".*");
                    i++;
                    break;
                case '?':
                    sb.Append('.');
                    i++;
                    break;
                case '\\' when i + 1 < pattern.Length:
                    // Redis glob escape: '\x' means literal 'x'
                    sb.Append(Regex.Escape(pattern[i + 1].ToString()));
                    i += 2;
                    break;
                case '[':
                    {
                        // Copy the character class as-is (translating to regex class syntax),
                        // Redis glob classes are already very close to regex classes.
                        var end = pattern.IndexOf(']', i + 1);
                        if (end == -1)
                        {
                            // No closing bracket: treat '[' as a literal.
                            sb.Append(Regex.Escape("["));
                            i++;
                        }
                        else
                        {
                            var classBody = pattern.Substring(i, end - i + 1); // includes [ and ]
                            // Redis uses '^' for negation same as regex; '-' for ranges same as regex.
                            sb.Append(classBody);
                            i = end + 1;
                        }
                        break;
                    }
                default:
                    sb.Append(Regex.Escape(c.ToString()));
                    i++;
                    break;
            }
        }
        sb.Append('$');
        return sb.ToString();
    }

    public async Task PublishInvalidationAsync(string key)
    {
        if (_redisSubscriber == null || !_redisConnected) return;

        try
        {
            var channel = new RedisChannel(GetInvalidationChannel(), RedisChannel.PatternMode.Literal);
            var message = _instanceId + key;
            await _redisSubscriber.PublishAsync(channel, message).ConfigureAwait(false);
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
    /// SCAN and DEL are performed separately (no Lua script), so DEL commands
    /// are routed by StackExchange.Redis per-key hash slot, which keeps this
    /// safe on both standalone and clustered deployments (as long as every
    /// master shard is scanned — see <paramref name="scanAllMasters"/>).
    /// Returns the number of keys actually deleted.
    /// </summary>
    public async Task<long> RemoveByPatternAsync(
        string pattern,
        bool scanAllMasters = true,
        CancellationToken cancellationToken = default)
    {
        if (!_redisConnected) return 0;

        const int scanBatchSize = 1000;
        const int deleteBatchSize = 500;

        long deletedCount = 0;

        var servers = scanAllMasters
            ? _redisConnection.GetEndPoints()
                .Select(ep => _redisConnection.GetServer(ep))
                .Where(s => !s.IsReplica)
                .ToList()
            : [_redisConnection.GetServer(_redisConnection.GetEndPoints().First())];

        foreach (var server in servers)
        {
            var buffer = new List<RedisKey>(deleteBatchSize);

            await foreach (var key in server.KeysAsync(
                                   database: _redisDb.Database,
                                   pattern: pattern,
                                   pageSize: scanBatchSize)
                               .WithCancellation(cancellationToken))
            {
                buffer.Add(key);

                if (buffer.Count >= deleteBatchSize)
                {
                    deletedCount += await _redisDb.KeyDeleteAsync(buffer.ToArray())
                        .WaitAsync(cancellationToken);
                    buffer.Clear();
                }
            }

            if (buffer.Count > 0)
            {
                deletedCount += await _redisDb.KeyDeleteAsync(buffer.ToArray())
                    .WaitAsync(cancellationToken);
            }
        }

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
            var message = _instanceId + pattern;
            await _redisSubscriber.PublishAsync(channel, message).ConfigureAwait(false);
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