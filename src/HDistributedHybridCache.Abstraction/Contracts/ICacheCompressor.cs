namespace HDistributedHybridCache.Abstraction.Contracts;

/// <summary>
/// Data compression abstraction to reduce size in Redis
/// </summary>
public interface ICacheCompressor
{
    byte[] Compress(byte[] data);
    byte[] Decompress(byte[] compressedData);
}