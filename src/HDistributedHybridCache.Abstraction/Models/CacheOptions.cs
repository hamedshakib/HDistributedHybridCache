using System.ComponentModel;

namespace HDistributedHybridCache.Abstraction.Models;

public record CacheOptions
{
    // ============ Memory Cache Settings ============

    /// <summary>
    /// Maximum capacity (number of items) for Memory Cache.
    /// </summary>
    /// <remarks>Default value: <c>1024</c></remarks>
    [DefaultValue(1024L)]
    public long MemoryCacheMaxSize { get; set; } = 1024;

    /// <summary>
    /// Default expiration time for cache items in memory.
    /// </summary>
    /// <remarks>
    /// Default value: <c>5 minutes</c>.
    /// In appsettings.json, enter as string: <c>"00:05:00"</c>
    /// </remarks>
    [DefaultValue("00:05:00")]
    public TimeSpan DefaultMemoryTtl { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Compaction percentage when cache is full.
    /// </summary>
    /// <remarks>Default value: <c>0.2</c> (meaning 20%)</remarks>
    [DefaultValue(0.2)]
    public double MemoryCacheCompactionPercentage { get; set; } = 0.2;

    // ============ Redis Settings ============

    /// <summary>
    /// Redis server connection address.
    /// </summary>
    /// <remarks>Default value: <c>localhost:6379</c></remarks>
    [DefaultValue("localhost:6379")]
    public string RedisConnectionString { get; set; } = "localhost:6379";

    /// <summary>
    /// Default expiration time for cache items in Redis.
    /// </summary>
    /// <remarks>
    /// Default value: <c>20 minutes</c>.
    /// In appsettings.json, enter as string: <c>"00:20:00"</c>
    /// </remarks>
    [DefaultValue("00:20:00")]
    public TimeSpan DefaultRedisTtl { get; set; } = TimeSpan.FromMinutes(20);

    /// <summary>
    /// Redis database number to use.
    /// </summary>
    /// <remarks>Default value: <c>0</c></remarks>
    [DefaultValue(0)]
    public int RedisDatabase { get; set; } = 0;

    /// <summary>
    /// Prefix for all keys stored in Redis.
    /// </summary>
    /// <remarks>Default value: <c>""</c> (no prefix)</remarks>
    [DefaultValue("")]
    public string KeyPrefix { get; set; } = "";

    // ============ Retry Settings ============

    /// <summary>
    /// Number of retry attempts on Redis connection failure.
    /// </summary>
    /// <remarks>Default value: <c>3</c></remarks>
    [DefaultValue(3)]
    public int RedisRetryCount { get; set; } = 3;

    /// <summary>
    /// Base delay (in milliseconds) between retry attempts.
    /// </summary>
    /// <remarks>Default value: <c>100</c> milliseconds</remarks>
    [DefaultValue(100)]
    public int RedisRetryBaseDelayMs { get; set; } = 100;

    /// <summary>
    /// Connection timeout to Redis (in milliseconds).
    /// </summary>
    /// <remarks>Default value: <c>5000</c> milliseconds</remarks>
    [DefaultValue(5000)]
    public int RedisConnectTimeoutMs { get; set; } = 5000;

    // ============ Pub/Sub Settings ============

    /// <summary>
    /// Channel name prefix for Pub/Sub invalidation messages.
    /// </summary>
    /// <remarks>Default value: <c>cache:invalidate</c></remarks>
    [DefaultValue("cache:invalidate")]
    public string PubSubChannelPrefix { get; set; } = "cache:invalidate";

    /// <summary>
    /// Should Pub/Sub be enabled for cache synchronization across multiple instances?
    /// </summary>
    /// <remarks>Default value: <c>true</c></remarks>
    [DefaultValue(true)]
    public bool EnablePubSub { get; set; } = true;

    // ============ HotKey Settings ============

    /// <summary>
    /// Number of requests a key must have within a time window to be identified as HotKey.
    /// </summary>
    /// <remarks>Default value: <c>10</c></remarks>
    [DefaultValue(10)]
    public int HotKeyThreshold { get; set; } = 10;

    /// <summary>
    /// Time window for HotKey identification.
    /// </summary>
    /// <remarks>
    /// Default value: <c>5 minutes</c>.
    /// In appsettings.json, enter as string: <c>"00:05:00"</c>
    /// </remarks>
    [DefaultValue("00:05:00")]
    public TimeSpan HotKeyDecayWindow { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Maximum number of HotKeys that can be tracked simultaneously.
    /// </summary>
    /// <remarks>Default value: <c>1000</c></remarks>
    [DefaultValue(1000)]
    public int MaxHotKeys { get; set; } = 1000;

    /// <summary>
    /// Should HotKey tracking be enabled?
    /// </summary>
    /// <remarks>Default value: <c>true</c></remarks>
    [DefaultValue(true)]
    public bool EnableHotKeyTracking { get; set; } = true;

    // ============ Statistics Settings ============

    /// <summary>
    /// Should cache statistics (like Hit/Miss) be recorded?
    /// </summary>
    /// <remarks>Default value: <c>true</c></remarks>
    [DefaultValue(true)]
    public bool EnableStatistics { get; set; } = true;

    /// <summary>
    /// Should statistics be calculated as Rolling Window?
    /// </summary>
    /// <remarks>Default value: <c>true</c></remarks>
    [DefaultValue(true)]
    public bool EnableRollingWindow { get; set; } = true;

    /// <summary>
    /// Time window for rolling statistics.
    /// </summary>
    /// <remarks>
    /// Default value: <c>1 minute</c>.
    /// In appsettings.json, enter as string: <c>"00:01:00"</c>
    /// </remarks>
    [DefaultValue("00:01:00")]
    public TimeSpan StatisticsRollingWindow { get; set; } = TimeSpan.FromMinutes(1);

    // ============ Performance Settings ============

    /// <summary>
    /// Should protection against Cache Stampede (concurrent requests for expired key) be enabled?
    /// </summary>
    /// <remarks>Default value: <c>true</c></remarks>
    [DefaultValue(true)]
    public bool EnableCacheStampedeProtection { get; set; } = true;

    /// <summary>
    /// Maximum time (in milliseconds) to hold the lock for stampede protection.
    /// </summary>
    /// <remarks>Default value: <c>5000</c> milliseconds</remarks>
    [DefaultValue(5000)]
    public int StampedeLockTimeoutMs { get; set; } = 5000;

    /// <summary>
    /// Should cache data be compressed before storing in Redis?
    /// </summary>
    /// <remarks>Default value: <c>false</c></remarks>
    [DefaultValue(false)]
    public bool EnableCompression { get; set; } = false;

    /// <summary>
    /// Time window for cleaning up expired stampede locks.
    /// </summary>
    /// <remarks>
    /// Default value: <c>10 minutes</c>.
    /// In appsettings.json, enter as string: <c>"00:10:00"</c>
    /// </remarks>
    [DefaultValue("00:10:00")]
    public TimeSpan StampedeLockCleanupInterval { get; set; } = TimeSpan.FromMinutes(10);

    // ============ Null Cache (Cache Poisoning Prevention) ============

    /// <summary>
    /// Should null values also be cached (to prevent Cache Poisoning and database attacks)?
    /// </summary>
    /// <remarks>Default value: <c>true</c></remarks>
    [DefaultValue(true)]
    public bool EnableNullCaching { get; set; } = true;

    /// <summary>
    /// Validity period for cached null values.
    /// </summary>
    /// <remarks>
    /// Default value: <c>30 seconds</c>.
    /// In appsettings.json, enter as string: <c>"00:00:30"</c>
    /// </remarks>
    [DefaultValue("00:00:30")]
    public TimeSpan NullCacheTtl { get; set; } = TimeSpan.FromSeconds(30);
}