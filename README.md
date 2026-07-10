# HDistributedHybridCache

[![.NET](https://img.shields.io/badge/.NET-10.0-blue.svg)](https://dotnet.microsoft.com/download/dotnet/10.0)
[![NuGet](https://img.shields.io/badge/nuget-available-brightgreen.svg)](https://www.nuget.org/packages/HDistributedHybridCache)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A high-performance distributed hybrid caching library for .NET that combines in-memory caching (L1) with Redis (L2) to provide optimal performance, scalability, and reliability.

## Features

- 🚀 **Two-Tier Caching**: Memory cache (L1) + Redis cache (L2) for optimal performance
- 🔥 **HotKey Tracking**: Automatically identifies and caches frequently accessed keys in memory
- 🛡️ **Cache Stampede Protection**: Prevents thundering herd problem with distributed locks
- 📡 **Pub/Sub Synchronization**: Real-time cache invalidation across multiple application instances
- ⭕ **Null Value Caching**: Prevents cache poisoning attacks by caching null values with configurable TTL
- 📦 **GZip Compression**: Optional compression to reduce Redis storage costs
- 📊 **Comprehensive Statistics**: Real-time monitoring with rolling window support
- 🔄 **Retry Policy**: Exponential backoff retry mechanism for Redis operations
- ⚙️ **Highly Configurable**: 20+ configuration options for fine-tuning

## Installation

Install the NuGet package:

```bash
dotnet add package HDistributedHybridCache
```

Or via Package Manager:

```powershell
Install-Package HDistributedHybridCache
```

## Configuration

### appsettings.json

```json
{
  "HDistributedHybridCache": {
    "RedisConnectionString": "localhost:6379",
    "RedisDatabase": 0,
    "KeyPrefix": "myapp",
    "DefaultRedisTtl": "00:20:00",
    "DefaultMemoryTtl": "00:05:00",
    "MemoryCacheMaxSize": 1024,
    "MemoryCacheCompactionPercentage": 0.2,
    "EnablePubSub": true,
    "PubSubChannelPrefix": "cache:invalidate",
    "RedisRetryCount": 3,
    "RedisRetryBaseDelayMs": 100,
    "RedisConnectTimeoutMs": 5000,
    "EnableHotKeyTracking": true,
    "HotKeyThreshold": 10,
    "HotKeyDecayWindow": "00:05:00",
    "MaxHotKeys": 1000,
    "EnableStatistics": true,
    "EnableRollingWindow": true,
    "StatisticsRollingWindow": "00:01:00",
    "EnableCacheStampedeProtection": true,
    "StampedeLockTimeoutMs": 5000,
    "StampedeLockCleanupInterval": "00:10:00",
    "EnableCompression": false,
    "EnableNullCaching": true,
    "NullCacheTtl": "00:00:30"
  }
}
```

### Dependency Injection

```csharp
using HDistributedHybridCache.Services;

var builder = WebApplication.CreateBuilder(args);

// Basic registration
builder.Services.AddHDistributedHybridCache(options =>
{
    options.RedisConnectionString = "localhost:6379";
    options.KeyPrefix = "myapp";
});

// Or with configuration from appsettings.json
builder.Services.AddHDistributedHybridCache(builder.Configuration
    .GetSection("HDistributedHybridCache")
    .Get<CacheOptions>());

var app = builder.Build();
```

## Usage

### Basic Operations

Inject the cache service:

```csharp
public class MyService
{
    private readonly ICacheService _cache;

    public MyService(ICacheService cache)
    {
        _cache = cache;
    }
}
```

### GetOrSet Pattern

```csharp
// Get or set with full cache key strategy
var cacheKey = CacheKey.PreferInMemory("user:123", TimeSpan.FromMinutes(10));

var user = await _cache.GetOrSetAsync(
    cacheKey,
    async ct => await GetUserFromDatabaseAsync(123, ct),
    cancellationToken
);
```

### Direct Redis Get

```csharp
// Get from Redis only (no L1 memory caching)
var value = await _cache.GetAsync<string>("user:123");
```

### Set with CacheKey

```csharp
// Set with different store strategies
await _cache.SetAsync(CacheKey.MustInMemory("config:settings", TimeSpan.FromHours(1)), configValue);
await _cache.SetAsync(CacheKey.HotKeyOnly("product:popular", TimeSpan.FromMinutes(30)), productValue);
await _cache.SetAsync(CacheKey.NeverInMemory("otp:abc123", TimeSpan.FromMinutes(5)), otpValue);
await _cache.SetAsync(CacheKey.PreferInMemory("user:profile", TimeSpan.FromMinutes(15)), profileValue);
```

### Remove

```csharp
// Remove from both memory and redis
await _cache.RemoveAsync(CacheKey.PreferInMemory("user:123"));
```

### Get Statistics

```csharp
var stats = _cache.GetStatistics();

Console.WriteLine($"Total Requests: {stats.TotalRequests}");
Console.WriteLine($"Memory Hit Rate: {stats.MemoryHitRate:F2}%");
Console.WriteLine($"Redis Hit Rate: {stats.RedisHitRate:F2}%");
Console.WriteLine($"Overall Hit Rate: {stats.OverallHitRate:F2}%");
Console.WriteLine($"Uptime: {stats.Uptime}");
```

## Cache Store Types

The library provides four storage strategies through the `CacheStoreType` enum:

### NeverInMemory

Never stores in memory cache (Redis only). Use for temporary data like OTPs and tokens.

```csharp
var cacheKey = CacheKey.NeverInMemory("otp:123", TimeSpan.FromMinutes(5));
```

### HotKeyOnly

Only stores in memory if the key becomes "hot" (frequently accessed). Default threshold is 10 accesses within 5 minutes.

```csharp
var cacheKey = CacheKey.HotKeyOnly("product:456", TimeSpan.FromMinutes(30));
```

### PreferInMemory

Always stores in memory with high priority. Memory TTL is capped to Redis TTL.

```csharp
var cacheKey = CacheKey.PreferInMemory("settings:global", TimeSpan.FromMinutes(15));
```

### MustInMemory

Must be stored in memory. Will evict other entries if cache is full (NeverRemove priority).

```csharp
var cacheKey = CacheKey.MustInMemory("security:config", TimeSpan.FromHours(1));
```

## Custom Serialization & Compression

### Custom Serializer

```csharp
// Register with custom serializer (e.g., System.Text.Json)
builder.Services.AddHDistributedHybridCache<MyCustomSerializer>(options =>
{
    options.EnableCompression = true;
});
```

### Custom Compressor

```csharp
// Register with custom serializer and compressor
builder.Services.AddHDistributedHybridCache<MySerializer, MyCompressor>(options =>
{
    options.EnableCompression = true;
    options.EnableNullCaching = true;
});
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Application Layer                     │
└─────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│                   ICacheService                          │
│              (Abstraction Interface)                   │
└─────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│            HDistributedHybridCacheService                │
│         (Main Implementation)                          │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────┐   │
│  │  StampedeProtector                              │   │
│  │  - Concurrent request protection                │   │
│  └─────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────┐   │
│  │  HotKeyTracker                                  │   │
│  │  - Identifies frequently accessed keys            │   │
│  └─────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────┐   │
│  │  RedisConnectionManager                         │   │
│  │  - Connection health monitoring                 │   │
│  │  - Pub/Sub invalidation handling              │   │
│  └─────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────┐   │
│  │  ICacheSerializer (Default: Newtonsoft)         │   │
│  ├─────────────────────────────────────────────────┤   │
│  │  ICacheCompressor (Default: GZip)               │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
                            │
      ┌───────────────────────┴───────────────────────┐
      ▼                                             ▼
┌─────────────────┐                    ┌──────────────────┐
│  Memory Cache   │                    │     Redis        │
│   (L1 Cache)    │                    │    (L2 Cache)    │
└─────────────────┘                    └──────────────────┘
```

## Complete Example

```csharp
using HDistributedHybridCache.Abstraction.Contracts;
using HDistributedHybridCache.Abstraction.Models;
using HDistributedHybridCache.Services;

// Program.cs
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddHDistributedHybridCache(options =>
{
    options.RedisConnectionString = "localhost:6379";
    options.KeyPrefix = "ecommerce";
    options.DefaultRedisTtl = TimeSpan.FromMinutes(20);
    options.MemoryCacheMaxSize = 2048;
    options.EnableCompression = true;
    options.EnableStatistics = true;
});

var app = builder.Build();

// Usage in a controller
app.MapGet("/api/products/{id}", async (ICacheService cache, int id) =>
{
    var cacheKey = CacheKey.HotKeyOnly($"product:{id}");

    var product = await cache.GetOrSetAsync(
        cacheKey,
        async ct => await Database.GetProductAsync(id),
        HttpContext.RequestAborted
    );

    return product is null ? Results.NotFound() : Results.Ok(product);
});

app.MapGet("/api/statistics", (ICacheService cache) =>
{
    var stats = cache.GetStatistics();
    return Results.Ok(new
    {
        stats.TotalRequests,
        stats.MemoryHits,
        stats.RedisHits,
        stats.Misses,
        MemoryHitRate = $"{stats.MemoryHitRate:F2}%",
        RedisHitRate = $"{stats.RedisHitRate:F2}%",
        OverallHitRate = $"{stats.OverallHitRate:F2}%"
    });
});

app.Run();
```

## Cache Statistics Snapshot

```csharp
var snapshot = _cache.GetStatistics().GetSnapshot();
// Returns:
// {
//   enabled: true,
//   uptime_minutes: 125.5,
//   total_requests: 15000,
//   memory_hits: 12000,
//   redis_hits: 2500,
//   misses: 500,
//   invalidations: 20,
//   memory_hit_rate: 80.00,
//   redis_hit_rate: 16.67,
//   overall_hit_rate: 96.67,
//   requests_per_minute: 850,
//   hits_per_minute: 830,
//   top_hot_keys: [["product:1", 500], ["user:123", 300]]
// }
```

## Development

### Prerequisites

- [.NET 10.0 SDK](https://dotnet.microsoft.com/download/dotnet/10.0)
- Redis Server (optional, for testing)

### Build

```bash
dotnet build
```

### Run Tests

```bash
dotnet test
```

### Pack NuGet

```bash
dotnet pack -c Release
```

## Configuration Reference

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `RedisConnectionString` | string | `"localhost:6379"` | Redis server connection string |
| `RedisDatabase` | int | `0` | Redis database number |
| `KeyPrefix` | string | `""` | Prefix for all Redis keys |
| `DefaultRedisTtl` | TimeSpan | `20 min` | Default expiration in Redis |
| `DefaultMemoryTtl` | TimeSpan | `5 min` | Default expiration in memory |
| `MemoryCacheMaxSize` | long | `1024` | Max items in memory cache |
| `MemoryCacheCompactionPercentage` | double | `0.2` | Compaction percentage (20%) |
| `EnablePubSub` | bool | `true` | Enable Pub/Sub invalidation |
| `PubSubChannelPrefix` | string | `"cache:invalidate"` | Pub/Sub channel prefix |
| `RedisRetryCount` | int | `3` | Redis retry attempts |
| `RedisRetryBaseDelayMs` | int | `100` | Base delay for retry (ms) |
| `RedisConnectTimeoutMs` | int | `5000` | Redis connection timeout (ms) |
| `EnableHotKeyTracking` | bool | `true` | Enable HotKey tracking |
| `HotKeyThreshold` | int | `10` | Accesses to become HotKey |
| `HotKeyDecayWindow` | TimeSpan | `5 min` | Window for HotKey threshold |
| `MaxHotKeys` | int | `1000` | Max HotKeys to track |
| `EnableStatistics` | bool | `true` | Enable statistics recording |
| `EnableRollingWindow` | bool | `true` | Enable rolling window stats |
| `StatisticsRollingWindow` | TimeSpan | `1 min` | Rolling window duration |
| `EnableCacheStampedeProtection` | bool | `true` | Prevent stampede attacks |
| `StampedeLockTimeoutMs` | int | `5000` | Lock timeout (ms) |
| `StampedeLockCleanupInterval` | TimeSpan | `10 min` | Lock cleanup interval |
| `EnableCompression` | bool | `false` | Enable GZip compression |
| `EnableNullCaching` | bool | `true` | Cache null values |
| `NullCacheTtl` | TimeSpan | `30 sec` | Null cache expiration |

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Author

**Hamed Shakib** - [GitHub](https://github.com/hamedshakib)

---

If you find this library useful, please consider giving it a ⭐ star on GitHub!