using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;
using TourDataOrchestrator.Application.Abstractions;
using TourDataOrchestrator.Storage.Configuration;
using TourDataOrchestrator.Storage.Stores;

namespace TourDataOrchestrator.Storage.Extensions;

public static class StorageServiceExtensions
{
    public static IServiceCollection AddStorage(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.Configure<RedisOptions>(
            configuration.GetSection(RedisOptions.SectionName));

        // IConnectionMultiplexer jako Singleton — ConnectionMultiplexer jest thread-safe
        // i przeznaczony do wielokrotnego użycia przez cały czas życia aplikacji.
        services.AddSingleton<IConnectionMultiplexer>(sp =>
        {
            var opts = configuration
                .GetSection(RedisOptions.SectionName)
                .Get<RedisOptions>() ?? new RedisOptions();

            return ConnectionMultiplexer.Connect(opts.ConnectionString);
        });

        services.AddSingleton<ITaskStateStore, RedisTaskStateStore>();
        services.AddSingleton<IScrapingResultAggregator, RedisScrapingResultAggregator>();
        services.AddSingleton<IProviderRegistry, RedisProviderRegistry>();

        return services;
    }
}
