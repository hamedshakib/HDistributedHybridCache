using HDistributedHybridCache.Abstraction.Models;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace HDistributedHybridCache.Infrastructures.Utilities;

/// <summary>
/// Handles Redis operation retries with exponential backoff.
/// </summary>
internal sealed class RetryPolicy
{
    private readonly int _retryCount;
    private readonly int _baseDelayMs;
    private readonly ILogger _logger;

    public RetryPolicy(CacheOptions options, ILogger logger)
    {
        _retryCount = options.RedisRetryCount;
        _baseDelayMs = options.RedisRetryBaseDelayMs;
        _logger = logger;
    }

    /// <summary>
    /// Executes an operation with retry logic and exponential backoff.
    /// </summary>
    public async Task<T> ExecuteWithRetryAsync<T>(Func<Task<T>> operation, CancellationToken cancellationToken)
    {
        Exception? lastException = null;

        for (int attempt = 0; attempt <= _retryCount; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                return await operation().ConfigureAwait(false);
            }
            catch (Exception ex) when (attempt < _retryCount && ex is not OperationCanceledException)
            {
                lastException = ex;
                _logger.LogWarning(ex,
                    "Redis operation failed (attempt {Attempt}/{MaxRetries}). Retrying...",
                    attempt + 1, _retryCount);

                var delay = TimeSpan.FromMilliseconds(_baseDelayMs * Math.Pow(2, attempt));
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }

        throw new RedisException($"Redis operation failed after {_retryCount + 1} attempts", lastException);
    }
}