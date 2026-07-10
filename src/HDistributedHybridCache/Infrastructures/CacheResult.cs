namespace HDistributedHybridCache.Infrastructures;

/// <summary>
/// Wrapper for cache lookup results to distinguish between "cache hit with null value" and "cache miss".
/// </summary>
internal readonly struct CacheResult<T>
{
    public readonly T? Value;
    public readonly bool HasValue; // true = cache hit (even if Value is null)

    private CacheResult(T? value, bool hasValue)
    {
        Value = value;
        HasValue = hasValue;
    }

    public static CacheResult<T> Hit(T? value) => new(value, true);
    public static CacheResult<T> Miss => default; // HasValue = false
}