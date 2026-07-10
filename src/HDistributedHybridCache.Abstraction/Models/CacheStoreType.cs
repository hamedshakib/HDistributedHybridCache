namespace HDistributedHybridCache.Abstraction.Models;

public enum CacheStoreType : byte
{
    /// <summary>
    /// Never store in Memory Cache (Redis only)
    /// Example: temporary tokens, OTP, one-time data
    /// </summary>
    NeverInMemory = 1,

    /// <summary>
    /// Only store in Memory if marked as HotKey
    /// Example: user sessions, popular products
    /// </summary>
    HotKeyOnly = 2,

    /// <summary>
    /// Always store in Memory (priority given to available memory)
    /// Example: system settings, reference data
    /// </summary>
    PreferInMemory = 3,

    /// <summary>
    /// Must be stored in Memory (evict others if cache is full)
    /// Example: critical data, security settings
    /// </summary>
    MustInMemory = 4
}