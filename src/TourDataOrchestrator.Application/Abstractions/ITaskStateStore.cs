using TourDataOrchestrator.Domain.Enums;

namespace TourDataOrchestrator.Application.Abstractions;

public interface ITaskStateStore
{
    Task InitializeAsync(string taskId, int expectedWorkerCount, TimeSpan ttl, CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically increments the received-response counter and returns the new count.
    /// </summary>
    Task<long> IncrementReceivedCountAsync(string taskId, CancellationToken cancellationToken = default);

    Task<int> GetExpectedCountAsync(string taskId, CancellationToken cancellationToken = default);

    Task SetStatusAsync(string taskId, OrchestratorTaskStatus status, CancellationToken cancellationToken = default);

    Task<OrchestratorTaskStatus?> GetStatusAsync(string taskId, CancellationToken cancellationToken = default);
}
