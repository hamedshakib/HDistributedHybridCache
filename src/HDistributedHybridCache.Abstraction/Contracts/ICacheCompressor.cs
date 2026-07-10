namespace HDistributedHybridCache.Abstraction.Contracts;

/// <summary>
/// انتزاع فشرده‌سازی داده‌ها برای کاهش حجم در Redis
/// </summary>
public interface ICacheCompressor
{
    byte[] Compress(byte[] data);
    byte[] Decompress(byte[] compressedData);
}