using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using TourDataOrchestrator.Application.Abstractions;
using TourDataOrchestrator.Messaging.Configuration;
using TourDataOrchestrator.Messaging.Consumer;
using TourDataOrchestrator.Messaging.Publisher;

namespace TourDataOrchestrator.Messaging.Extensions;

public static class MessagingServiceExtensions
{
    public static IServiceCollection AddMessaging(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.Configure<RabbitMqOptions>(
            configuration.GetSection(RabbitMqOptions.SectionName));

        // Singleton: jedno połączenie TCP współdzielone przez wszystkie publikacje.
        services.AddSingleton<IMessagePublisher, RabbitMqMessagePublisher>();

        // Hosted service: consumer działa przez cały czas życia aplikacji.
        services.AddHostedService<ResultsConsumerService>();

        return services;
    }
}
