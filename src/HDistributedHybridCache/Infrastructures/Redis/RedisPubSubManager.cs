using HDistributedHybridCache.Abstraction.Models;
using HDistributedHybridCache.Services;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System.Collections.Concurrent;
using System.Text;
using System.Text.RegularExpressions;

namespace HDistributedHybridCache.Infrastructures.Redis;

/// <summary>
/// Manages Redis Pub/Sub subscription and invalidation events.
/// </summary>
internal sealed class RedisPubSubManager : IDisposable
{
    private readonly ISubscriber _redisSubscriber;
    private readonly CacheOptions _options;
    private readonly ILogger _logger;
    private readonly IMemoryCache _memoryCache;
    private readonly HotKeyTracker _hotKeyTracker;
    private readonly CacheStatistics _statistics;
    private readonly ConcurrentDictionary<string, Lazy<SemaphoreSlim>> _stampedeLocks;
    private readonly RedisKeyHelper _keyHelper;

    private readonly string _instanceId = Guid.NewGuid().ToString("N");
    private const int InstanceIdLength = 32;

    // Cache of compiled regexes per pattern to avoid re-building/re-compiling
    private readonly ConcurrentDictionary<string, Regex> _patternRegexCache = new();

    private volatile bool _subscribed;

    public string InstanceId => _instanceId;

    public RedisPubSubManager(
        IConnectionMultiplexer redisConnection,
        CacheOptions options,
        ILogger logger,
        IMemoryCache memoryCache,
        HotKeyTracker hotKeyTracker,
        CacheStatistics statistics,
        ConcurrentDictionary<string, Lazy<SemaphoreSlim>> stampedeLocks,
        RedisKeyHelper keyHelper)
    {
        _options = options;
        _logger = logger;
        _memoryCache = memoryCache;
        _hotKeyTracker = hotKeyTracker;
        _statistics = statistics;
        _stampedeLocks = stampedeLocks;
        _keyHelper = keyHelper;

        _redisSubscriber = redisConnection.GetSubscriber();

        if (options.EnablePubSub)
        {
            SubscribeToInvalidationEventsAsync().GetAwaiter().GetResult();
        }
    }

    private string GetInvalidationChannel() => $"{_options.PubSubChannelPrefix}:invalidate:key";

    private string GetPatternInvalidationChannel() => $"{_options.PubSubChannelPrefix}:invalidate:pattern";

    private async Task SubscribeToInvalidationEventsAsync()
    {
        await _redisSubscriber.UnsubscribeAllAsync().ConfigureAwait(false);

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
        var sb = new StringBuilder("^");
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
        if (!_subscribed) return;

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

    public async Task PublishPatternInvalidationAsync(string pattern, CancellationToken cancellationToken = default)
    {
        if (!_subscribed) return;

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

    public async Task ReSubscribeAsync()
    {
        if (_options.EnablePubSub)
        {
            try
            {
                await SubscribeToInvalidationEventsAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to re-subscribe to invalidation events.");
            }
        }
    }

    public void Dispose()
    {
        _redisSubscriber.UnsubscribeAllAsync().GetAwaiter().GetResult();
    }
}