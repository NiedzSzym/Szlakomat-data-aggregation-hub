namespace TourDataOrchestrator.Messaging.Configuration;

public sealed class RabbitMqOptions
{
    public const string SectionName = "RabbitMq";

    public string Host { get; init; } = "localhost";
    public int Port { get; init; } = 5672;
    public string Username { get; init; } = "guest";
    public string Password { get; init; } = "guest";
    public string VirtualHost { get; init; } = "/";

    /// <summary>
    /// Topic Exchange, na który Orkiestrator publikuje zadania dla workerów.
    /// </summary>
    public string TaskExchangeName { get; init; } = "orchestrator.tasks";

    /// <summary>
    /// Kolejka, na której Orkiestrator nasłuchuje wyników (gather leg).
    /// </summary>
    public string ResultsQueueName { get; init; } = "orchestrator.results";
}
