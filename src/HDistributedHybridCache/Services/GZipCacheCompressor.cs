using HDistributedHybridCache.Abstraction.Contracts;
using System.IO.Compression;

namespace HDistributedHybridCache.Services;

/// <summary>
/// GZip compression implementation
/// </summary>
public class GZipCacheCompressor : ICacheCompressor
{
    public byte[] Compress(byte[] data)
    {
        if (data is null || data.Length == 0)
            return data ?? [];

        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Fastest, leaveOpen: true))
        {
            gzip.Write(data, 0, data.Length);
            gzip.Flush();
        }
        return output.ToArray();
    }

    public byte[] Decompress(byte[] compressedData)
    {
        if (compressedData is null || compressedData.Length == 0)
            return compressedData ?? [];

        using var input = new MemoryStream(compressedData);
        using var gzip = new GZipStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        gzip.CopyTo(output);
        return output.ToArray();
    }
}