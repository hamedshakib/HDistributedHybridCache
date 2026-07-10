using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace HDistributedHybridCache.Abstraction.Models;

public class CacheStatistics
{
    private readonly CacheOptions _options;
    private readonly bool _isEnabled;

    // شمارنده‌های تجمعی
    private long _totalRequests;
    private long _memoryHits;
    private long _redisHits;
    private long _misses;
    private long _invalidations;
    private readonly DateTime _startTime = DateTime.UtcNow;

    // پنجره‌های غلتان (اختیاری)
    private readonly RollingWindow? _requestsWindow;
    private readonly RollingWindow? _hitsWindow;

    // کلیدهای داغ برای آمار
    private readonly ConcurrentDictionary<string, long> _hotKeyStats = new();
    private readonly int _maxHotKeyStats = 100;

    public CacheStatistics(CacheOptions options)
    {
        _options = options;
        _isEnabled = options.EnableStatistics;

        if (_isEnabled && options.EnableRollingWindow)
        {
            _requestsWindow = new RollingWindow(options.StatisticsRollingWindow);
            _hitsWindow = new RollingWindow(options.StatisticsRollingWindow);
        }
    }

    // ============ Properties ============
    public bool IsEnabled => _isEnabled;
    public long TotalRequests => Interlocked.Read(ref _totalRequests);
    public long MemoryHits => Interlocked.Read(ref _memoryHits);
    public long RedisHits => Interlocked.Read(ref _redisHits);
    public long Misses => Interlocked.Read(ref _misses);
    public long Invalidations => Interlocked.Read(ref _invalidations);
    public TimeSpan Uptime => DateTime.UtcNow - _startTime;

    public double MemoryHitRate => TotalRequests > 0
        ? (double)MemoryHits / TotalRequests * 100
        : 0;

    public double RedisHitRate => TotalRequests > 0
        ? (double)RedisHits / TotalRequests * 100
        : 0;

    public double OverallHitRate => TotalRequests > 0
        ? (double)(MemoryHits + RedisHits) / TotalRequests * 100
        : 0;

    public long RequestsPerMinute => _isEnabled && _options.EnableRollingWindow
        ? _requestsWindow?.GetCount() ?? 0
        : 0;

    public long HitsPerMinute => _isEnabled && _options.EnableRollingWindow
        ? _hitsWindow?.GetCount() ?? 0
        : 0;

    // ============ Record Methods ============
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RecordRequest()
    {
        if (!_isEnabled) return;
        Interlocked.Increment(ref _totalRequests);
        _requestsWindow?.Record();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RecordMemoryHit(string key)
    {
        if (!_isEnabled) return;
        Interlocked.Increment(ref _memoryHits);
        _hitsWindow?.Record();
        RecordHotKeyStat(key);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RecordRedisHit(string key)
    {
        if (!_isEnabled) return;
        Interlocked.Increment(ref _redisHits);
        _hitsWindow?.Record();
        RecordHotKeyStat(key);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RecordMiss(string key)
    {
        if (!_isEnabled) return;
        Interlocked.Increment(ref _misses);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RecordInvalidation()
    {
        if (!_isEnabled) return;
        Interlocked.Increment(ref _invalidations);
    }

    private void RecordHotKeyStat(string key)
    {
        if (!_isEnabled || string.IsNullOrEmpty(key)) return;

        _hotKeyStats.AddOrUpdate(key, 1, (_, count) => count + 1);

        if (_hotKeyStats.Count > _maxHotKeyStats)
        {
            TrimHotKeyStats();
        }
    }

    private void TrimHotKeyStats()
    {
        var toRemove = _hotKeyStats
            .OrderBy(kv => kv.Value)
            .Take(10)
            .Select(kv => kv.Key)
            .ToList();

        foreach (var key in toRemove)
        {
            _hotKeyStats.TryRemove(key, out _);
        }
    }

    /// <summary>
    /// بازنشانی تمام آمار (مثلاً هنگام قطعی Redis)
    /// </summary>
    public void Reset()
    {
        Interlocked.Exchange(ref _totalRequests, 0);
        Interlocked.Exchange(ref _memoryHits, 0);
        Interlocked.Exchange(ref _redisHits, 0);
        Interlocked.Exchange(ref _misses, 0);
        Interlocked.Exchange(ref _invalidations, 0);

        _requestsWindow?.Clear();
        _hitsWindow?.Clear();
        _hotKeyStats.Clear();
    }

    // ============ Get Methods ============
    public IReadOnlyList<KeyValuePair<string, long>> GetTopHotKeys(int count = 10)
    {
        if (!_isEnabled) return Array.Empty<KeyValuePair<string, long>>();

        return _hotKeyStats
            .OrderByDescending(kv => kv.Value)
            .Take(count)
            .ToList();
    }

    public Dictionary<string, object> GetSnapshot()
    {
        if (!_isEnabled)
        {
            return new Dictionary<string, object> { ["enabled"] = false };
        }

        return new Dictionary<string, object>
        {
            ["enabled"] = true,
            ["uptime_minutes"] = Math.Round(Uptime.TotalMinutes, 2),
            ["total_requests"] = TotalRequests,
            ["memory_hits"] = MemoryHits,
            ["redis_hits"] = RedisHits,
            ["misses"] = Misses,
            ["invalidations"] = Invalidations,
            ["memory_hit_rate"] = Math.Round(MemoryHitRate, 2),
            ["redis_hit_rate"] = Math.Round(RedisHitRate, 2),
            ["overall_hit_rate"] = Math.Round(OverallHitRate, 2),
            ["requests_per_minute"] = RequestsPerMinute,
            ["hits_per_minute"] = HitsPerMinute,
            ["top_hot_keys"] = GetTopHotKeys(5)
        };
    }

    // ============ Rolling Window Helper ============
    private class RollingWindow
    {
        private readonly ConcurrentQueue<DateTime> _timestamps = new();
        private readonly TimeSpan _windowSize;
        private long _count;

        public RollingWindow(TimeSpan windowSize)
        {
            _windowSize = windowSize;
        }

        public void Record()
        {
            var now = DateTime.UtcNow;
            _timestamps.Enqueue(now);
            Interlocked.Increment(ref _count);
            Cleanup(now);
        }

        public long GetCount()
        {
            Cleanup(DateTime.UtcNow);
            return Interlocked.Read(ref _count);
        }

        public void Clear()
        {
            while (_timestamps.TryDequeue(out _)) { }
            Interlocked.Exchange(ref _count, 0);
        }

        private void Cleanup(DateTime now)
        {
            while (_timestamps.TryPeek(out var oldest) && now - oldest > _windowSize)
            {
                if (_timestamps.TryDequeue(out _))
                {
                    Interlocked.Decrement(ref _count);
                }
            }
        }
    }
}