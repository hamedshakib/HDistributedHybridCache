using HDistributedHybridCache.Abstraction.Contracts;
using HDistributedHybridCache.Abstraction.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace HDistributedHybridCache.Services;

/// <summary>
/// Extension methods for registering the distributed hybrid cache service.
/// </summary>
public static class ServiceCollectionExtensions
{
    extension(IServiceCollection services)
    {
        /// <summary>
        /// Registers the distributed hybrid cache service with default serializer and optional compression.
        /// </summary>
        public IServiceCollection AddHDistributedHybridCache(Action<CacheOptions>? configureOptions = null)
        {
            // Ensure logging services are registered
            services.AddLogging();

            var options = new CacheOptions();
            configureOptions?.Invoke(options);
            services.AddSingleton(Options.Create(options));
            services.AddMemoryCache(memoryOptions =>
            {
                memoryOptions.SizeLimit = options.MemoryCacheMaxSize;
                memoryOptions.CompactionPercentage = options.MemoryCacheCompactionPercentage;
            });

            services.TryAddSingleton<IConnectionMultiplexer>(sp =>
            {
                try
                {
                    var config = new ConfigurationOptions
                    {
                        EndPoints = { options.RedisConnectionString },
                        ConnectTimeout = options.RedisConnectTimeoutMs,
                        AbortOnConnectFail = false,
                        ConnectRetry = options.RedisRetryCount
                    };
                    return ConnectionMultiplexer.Connect(config);
                }
                catch (Exception ex)
                {
                    var logger = sp.GetService<Microsoft.Extensions.Logging.ILogger<IConnectionMultiplexer>>();
                    logger?.LogWarning(ex, "Redis connection failed during startup. Cache will operate in degraded mode until Redis is available.");
                    return ConnectionMultiplexer.Connect(new ConfigurationOptions
                    {
                        EndPoints = { options.RedisConnectionString },
                        ConnectTimeout = 5000,
                        AbortOnConnectFail = false,
                        ConnectRetry = 0
                    });
                }
            });

            // Register default Serializer and Compressor (only if not already registered)
            services.TryAddSingleton<ICacheSerializer, NewtonsoftCacheSerializer>();
            if (options.EnableCompression)
            {
                services.TryAddSingleton<ICacheCompressor, GZipCacheCompressor>();
            }

            services.AddSingleton<ICacheService, CacheService>();

            return services;
        }

        /// <summary>
        /// Registers the cache service with a custom serializer type.
        /// </summary>
        public IServiceCollection AddHDistributedHybridCache<T>(Action<CacheOptions>? configureOptions = null)
            where T : class, ICacheSerializer
        {
            services.AddHDistributedHybridCache(configureOptions);
            services.TryAddSingleton<ICacheSerializer, T>();
            return services;
        }

        /// <summary>
        /// Registers the cache service with custom serializer and compressor types.
        /// </summary>
        public IServiceCollection AddHDistributedHybridCache<TSerializer, TCompressor>(Action<CacheOptions>? configureOptions = null)
            where TSerializer : class, ICacheSerializer
            where TCompressor : class, ICacheCompressor
        {
            services.AddHDistributedHybridCache(configureOptions);
            services.TryAddSingleton<ICacheSerializer, TSerializer>();
            services.TryAddSingleton<ICacheCompressor, TCompressor>();
            return services;
        }

        /// <summary>
        /// Registers the cache service with statistics and rolling window disabled.
        /// </summary>
        public IServiceCollection AddHDistributedHybridCacheWithoutStats(Action<CacheOptions>? configureOptions = null)
        {
            return services.AddHDistributedHybridCache(options =>
            {
                configureOptions?.Invoke(options);
                options.EnableStatistics = false;
                options.EnableRollingWindow = false;
            });
        }
    }
}