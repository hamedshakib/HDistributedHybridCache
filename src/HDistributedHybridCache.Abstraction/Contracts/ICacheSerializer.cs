namespace HDistributedHybridCache.Abstraction.Contracts;

/// <summary>
/// انتزاع سریالایزر برای کش
/// امکان جایگزینی Newtonsoft.Json با System.Text.Json، MessagePack و غیره
/// </summary>
public interface ICacheSerializer
{
    byte[] Serialize<T>(T value);
    T? Deserialize<T>(byte[] data);
}