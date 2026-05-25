using TourDataOrchestrator.Application.Abstractions;
using TourDataOrchestrator.Application.DTOs;

namespace TourDataOrchestrator.Api.Infrastructure;

// Tymczasowy stub zaspokajający zależność DI do czasu dostarczenia docelowej implementacji
// IScrapingResultAggregator z upstream systemu. Rejestruje odebrane wyniki wyłącznie w logach.
internal sealed class NullScrapingResultAggregator : IScrapingResultAggregator
{
    private readonly ILogger<NullScrapingResultAggregator> _logger;

    public NullScrapingResultAggregator(ILogger<NullScrapingResultAggregator> logger)
        => _logger = logger;

    public Task AggregateAsync(string taskId, WorkerResultMessage result, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation(
            "[STUB] Otrzymano wynik od workera '{Target}' dla zadania {TaskId}. Sukces: {Success}",
            result.Target, result.TaskId, result.Success);

        return Task.CompletedTask;
    }

    public Task<AggregatedResult?> GetAggregatedResultAsync(string taskId, CancellationToken cancellationToken = default)
    {
        _logger.LogWarning("[STUB] GetAggregatedResultAsync wywołane dla {TaskId} — stub zwraca null.", taskId);
        return Task.FromResult<AggregatedResult?>(null);
    }
}
