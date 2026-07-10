using HDistributedHybridCache.Abstraction.Contracts;
using System.IO.Compression;

namespace HDistributedHybridCache.Services;

/// <summary>
/// پیاده‌سازی فشرده‌سازی با GZip
/// </summary>
public class GZipCacheCompressor : ICacheCompressor
{
    public byte[] Compress(byte[] data)
    {
        if (data == null || data.Length == 0)
            return data ?? Array.Empty<byte>();

        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Fastest, leaveOpen: false))
        {
            gzip.Write(data, 0, data.Length);
        }
        return output.ToArray();
    }

    public byte[] Decompress(byte[] compressedData)
    {
        if (compressedData == null || compressedData.Length == 0)
            return compressedData ?? Array.Empty<byte>();

        using var input = new MemoryStream(compressedData);
        using var gzip = new GZipStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        gzip.CopyTo(output);
        return output.ToArray();
    }
}