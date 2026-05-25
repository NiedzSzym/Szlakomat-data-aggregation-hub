namespace TourDataOrchestrator.MockWorker.Configuration;

public sealed class MockWorkerOptions
{
    public const string SectionName = "MockWorker";

    public string Host { get; init; } = "localhost";
    public int Port { get; init; } = 5672;
    public string Username { get; init; } = "guest";
    public string Password { get; init; } = "guest";
    public string VirtualHost { get; init; } = "/";

    /// <summary>
    /// Kolejka dedykowana temu workerowi. Każdy provider powinien mieć unikalną nazwę.
    /// </summary>
    public string QueueName { get; init; } = "worker.mock";

    /// <summary>
    /// Routing Key pattern na Topic Exchange. "task.#" łapie wszystkie zadania —
    /// produkcyjny worker użyłby wzorca specyficznego dla swojego zasobu,
    /// np. "task.*.attraction_wawel".
    /// </summary>
    public string BindingKey { get; init; } = "task.#";

    public string TaskExchangeName { get; init; } = "orchestrator.tasks";
}
