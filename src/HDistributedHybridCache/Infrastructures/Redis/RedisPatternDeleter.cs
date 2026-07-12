using HDistributedHybridCache.Abstraction.Models;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace HDistributedHybridCache.Infrastructures.Redis;

/// <summary>
/// Handles pattern-based deletion from Redis using SCAN.
/// </summary>
internal sealed class RedisPatternDeleter
{
    private readonly IDatabase _redisDb;
    private readonly ILogger _logger;
    private readonly CacheOptions _options;

    public IDatabase RedisDb => _redisDb;

    public RedisPatternDeleter(IDatabase redisDb, ILogger logger, CacheOptions options)
    {
        _redisDb = redisDb;
        _logger = logger;
        _options = options;
    }

    /// <summary>
    /// Removes all keys matching the specified pattern from Redis using SCAN.
    /// SCAN and DEL are performed separately (no Lua script), so DEL commands
    /// are routed by StackExchange.Redis per-key hash slot, which keeps this
    /// safe on both standalone and clustered deployments (as long as every
    /// master shard is scanned — see <paramref name="scanAllMasters"/>).
    /// Returns the number of keys actually deleted.
    /// </summary>
    public async Task<long> RemoveByPatternAsync(
        string pattern,
        bool scanAllMasters = true,
        CancellationToken cancellationToken = default)
    {
        if (!_options.EnablePubSub)
        {
            // Skip if Redis is disconnected - caller will check connection first
            return 0;
        }

        const int scanBatchSize = 1000;
        const int deleteBatchSize = 500;

        long deletedCount = 0;

        var redisConnection = _redisDb.Multiplexer;
        if (!redisConnection.IsConnected)
        {
            _logger.LogWarning("Redis is disconnected. Skipping pattern deletion.");
            return 0;
        }

        var servers = scanAllMasters
            ? redisConnection.GetEndPoints()
                .Select(ep => redisConnection.GetServer(ep))
                .Where(s => !s.IsReplica)
                .ToList()
            : [redisConnection.GetServer(redisConnection.GetEndPoints().First())];

        foreach (var server in servers)
        {
            var buffer = new List<RedisKey>(deleteBatchSize);

            await foreach (var key in server.KeysAsync(
                                   database: _redisDb.Database,
                                   pattern: pattern,
                                   pageSize: scanBatchSize)
                               .WithCancellation(cancellationToken))
            {
                buffer.Add(key);

                if (buffer.Count >= deleteBatchSize)
                {
                    deletedCount += await _redisDb.KeyDeleteAsync(buffer.ToArray())
                        .WaitAsync(cancellationToken);
                    buffer.Clear();
                }
            }

            if (buffer.Count > 0)
            {
                deletedCount += await _redisDb.KeyDeleteAsync(buffer.ToArray())
                    .WaitAsync(cancellationToken);
            }
        }

        _logger.LogDebug("Deleted {Count} keys matching pattern '{Pattern}'", deletedCount, pattern);
        return deletedCount;
    }
}