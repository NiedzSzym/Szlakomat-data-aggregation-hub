using TourDataOrchestrator.Application.DTOs;

namespace TourDataOrchestrator.Application.Abstractions;

public interface IScrapingResultAggregator
{
    Task AggregateAsync(string taskId, WorkerResultMessage result, CancellationToken cancellationToken = default);

    Task<AggregatedResult?> GetAggregatedResultAsync(string taskId, CancellationToken cancellationToken = default);
}
