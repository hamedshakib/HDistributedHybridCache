using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace HDistributedHybridCache.Infrastructures;

/// <summary>
/// Protects against cache stampede (thundering herd) by serializing concurrent factory calls for the same key.
/// </summary>
internal sealed class StampedeProtector : IDisposable
{
    private readonly ConcurrentDictionary<string, Lazy<SemaphoreSlim>> _locks = new();
    private readonly int _lockTimeoutMs;
    private readonly ILogger _logger;
    private readonly Timer? _cleanupTimer;

    public StampedeProtector(bool enabled, int lockTimeoutMs, TimeSpan cleanupInterval, ILogger logger)
    {
        _lockTimeoutMs = lockTimeoutMs;
        _logger = logger;

        if (enabled)
        {
            _cleanupTimer = new Timer(CleanupStaleLocks, null, cleanupInterval, cleanupInterval);
        }
    }

    public async Task<T> ExecuteAsync<T>(
        string key,
        Func<CancellationToken, Task<CacheResult<T>>> getOrSetFromCache,
        Func<CancellationToken, Task<T>> factory,
        CancellationToken cancellationToken)
    {
        // First try cache without lock
        var result = await getOrSetFromCache(cancellationToken).ConfigureAwait(false);
        if (result.HasValue)
            return result.Value!;

        var keyLock = _locks.GetOrAdd(key, _ => new Lazy<SemaphoreSlim>(() => new SemaphoreSlim(1, 1)));

        if (!await keyLock.Value.WaitAsync(_lockTimeoutMs, cancellationToken).ConfigureAwait(false))
        {
            throw new TimeoutException($"Cache stampede lock timeout for key: {key}");
        }

        try
        {
            // Double-check after acquiring lock
            result = await getOrSetFromCache(cancellationToken).ConfigureAwait(false);
            if (result.HasValue)
                return result.Value!;

            var value = await factory(cancellationToken).ConfigureAwait(false);
            return value;
        }
        finally
        {
            try { keyLock.Value.Release(); }
            catch (ObjectDisposedException) { }
            catch (SemaphoreFullException) { }
        }
    }

    public void RemoveKey(string key) => _locks.TryRemove(key, out _);

    private void CleanupStaleLocks(object? state)
    {
        try
        {
            var keysToRemove = new List<string>();
            foreach (var kvp in _locks)
            {
                if (!kvp.Value.IsValueCreated)
                {
                    keysToRemove.Add(kvp.Key);
                }
            }

            foreach (var key in keysToRemove)
            {
                _locks.TryRemove(key, out _);
            }

            if (keysToRemove.Count > 0)
            {
                _logger.LogDebug("Cleaned up {Count} unused stampede locks.", keysToRemove.Count);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during stampede lock cleanup.");
        }
    }

    public void Dispose()
    {
        _cleanupTimer?.Dispose();

        foreach (var kvp in _locks)
        {
            if (kvp.Value.IsValueCreated)
            {
                kvp.Value.Value.Dispose();
            }
        }
        _locks.Clear();
    }
}