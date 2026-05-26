namespace TourDataOrchestrator.MockWorker.Configuration;

public sealed class MockWorkerOptions
{
    public const string SectionName = "MockWorker";

    public string Host { get; init; } = "localhost";
    public int Port { get; init; } = 5672;
    public string Username { get; init; } = "guest";
    public string Password { get; init; } = "guest";
    public string VirtualHost { get; init; } = "/";

    public string QueueName { get; init; } = "worker.mock";

    /// <summary>
    /// Routing Key pattern na Topic Exchange. "task.#" łapie wszystkie zadania —
    /// produkcyjny worker używa wzorca specyficznego dla swojej operacji,
    /// np. "task.pricing.#" lub "task.events.attraction_wawel".
    /// </summary>
    public string BindingKey { get; init; } = "task.#";

    public string TaskExchangeName { get; init; } = "orchestrator.tasks";

    public string ProviderId { get; init; } = "mock-worker";

    public IReadOnlyList<string> SupportedTargets { get; init; } =
        ["attraction_wawel", "attraction_wieliczka", "attraction_auschwitz", "transport_mpk"];

    public string Description { get; init; } = "Mock data provider — obsługuje wszystkie operacje i targety (task.#)";
}
