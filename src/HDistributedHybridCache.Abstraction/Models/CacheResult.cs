namespace HDistributedHybridCache.Abstraction.Models;

/// <summary>
/// Represents the result of a cache operation, distinguishing between hits (including null) and misses.
/// </summary>
/// <typeparam name="T">The type of value in the cache.</typeparam>
public readonly record struct CacheResult<T>
{
    /// <summary>
    /// Indicates whether the cache operation was a hit (value found) or a miss (value not found).
    /// Note: A hit can have a null value if null caching is enabled.
    /// </summary>
    public bool HasValue { get; }

    /// <summary>
    /// The cached value, or default(T) if HasValue is false.
    /// </summary>
    public T? Value { get; }

    private CacheResult(T? value)
    {
        HasValue = true;
        Value = value;
    }

    public CacheResult()
    {
        HasValue = false;
        Value = default;
    }

    /// <summary>
    /// Creates a cache result indicating a hit with the specified value.
    /// </summary>
    /// <param name="value">The cached value (can be null if null caching is enabled).</param>
    /// <returns>A cache result indicating a hit.</returns>
    public static CacheResult<T> Hit(T? value) => new CacheResult<T>(value);

    /// <summary>
    /// Creates a cache result indicating a miss.
    /// </summary>
    public static CacheResult<T> Miss => new CacheResult<T>();
}