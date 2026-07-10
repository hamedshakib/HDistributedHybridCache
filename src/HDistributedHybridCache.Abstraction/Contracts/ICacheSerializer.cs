namespace HDistributedHybridCache.Abstraction.Contracts;

/// <summary>
/// Cache serializer abstraction
/// Allows replacing Newtonsoft.Json with System.Text.Json, MessagePack, etc.
/// </summary>
public interface ICacheSerializer
{
    byte[] Serialize<T>(T value);
    T? Deserialize<T>(byte[] data);
}