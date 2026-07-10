using HDistributedHybridCache.Abstraction.Models;
using System.Collections.Concurrent;

namespace HDistributedHybridCache.Services;

internal class HotKeyTracker
{
    private readonly ConcurrentDictionary<string, (long Count, DateTime LastAccess)> _stats = new();
    private readonly ConcurrentDictionary<string, byte> _hotKeys = new();
    private readonly int _threshold;
    private readonly TimeSpan _decayWindow;
    private readonly int _maxHotKeys;
    private readonly bool _isEnabled;
    private readonly Lock _trimLock = new();
    private DateTime _lastTrim = DateTime.UtcNow;

    public HotKeyTracker(CacheOptions options)
    {
        _isEnabled = options.EnableHotKeyTracking;
        _threshold = options.HotKeyThreshold;
        _decayWindow = options.HotKeyDecayWindow;
        _maxHotKeys = options.MaxHotKeys;
    }

    public bool IsEnabled => _isEnabled;

    public void RecordAccess(string key, bool trackHotKey = true)
    {
        if (!_isEnabled || !trackHotKey || string.IsNullOrEmpty(key))
            return;

        var now = DateTime.UtcNow;

        _stats.AddOrUpdate(key,
            (1, now),
            (_, old) =>
            {
                if (now - old.LastAccess > _decayWindow)
                {
                    return (1, now);
                }
                return (old.Count + 1, now);
            }
        );

        // Check if key became hot
        if (_stats.TryGetValue(key, out var stat) && stat.Count >= _threshold)
        {
            _hotKeys.TryAdd(key, 0);

            // If too many hot keys, trim them (check inside lock to avoid race condition)
            TrimHotKeysIfNeeded();
        }
    }

    public bool IsHotKey(string key)
    {
        if (!_isEnabled || string.IsNullOrEmpty(key))
            return false;

        return _hotKeys.ContainsKey(key);
    }

    public void RemoveKey(string key)
    {
        if (string.IsNullOrEmpty(key)) return;
        _stats.TryRemove(key, out _);
        _hotKeys.TryRemove(key, out _);
    }

    private void TrimHotKeysIfNeeded()
    {
        var thirtySecondsAgo = TimeSpan.FromSeconds(30);

        if (_hotKeys.Count <= _maxHotKeys || DateTime.UtcNow - _lastTrim < thirtySecondsAgo)
            return;

        lock (_trimLock)
        {
            if (DateTime.UtcNow - _lastTrim < thirtySecondsAgo)
                return;

            if (_hotKeys.Count <= _maxHotKeys)
                return;

            TrimHotKeys();
        }
    }

    private void TrimHotKeys()
    {
        var topKeys = _stats
            .OrderByDescending(kv => kv.Value.Count)
            .Take(_maxHotKeys)
            .Select(kv => kv.Key)
            .ToHashSet();

        foreach (var key in _hotKeys.Keys.ToList())
        {
            if (!topKeys.Contains(key))
            {
                _hotKeys.TryRemove(key, out _);
            }
        }

        _lastTrim = DateTime.UtcNow;
    }

    public void Cleanup()
    {
        if (!_isEnabled) return;

        var cutoff = DateTime.UtcNow.Subtract(TimeSpan.FromHours(1));
        var staleKeys = _stats
            .Where(kvp => kvp.Value.LastAccess < cutoff)
            .Select(kvp => kvp.Key)
            .ToList();

        foreach (var key in staleKeys)
        {
            _stats.TryRemove(key, out _);
            _hotKeys.TryRemove(key, out _);
        }
    }
}