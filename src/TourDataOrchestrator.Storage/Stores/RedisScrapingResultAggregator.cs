using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Text.Json;
using TourDataOrchestrator.Application.Abstractions;
using TourDataOrchestrator.Application.DTOs;
using TourDataOrchestrator.Domain.Enums;
using TourDataOrchestrator.Storage.Configuration;

namespace TourDataOrchestrator.Storage.Stores;

/// <summary>
/// Implementacja <see cref="IScrapingResultAggregator"/> oparta na Redis.
///
/// Schemat kluczy:
///   task:{taskId}           → Hash (expected, received, status)   [zarządza ITaskStateStore]
///   task:{taskId}:results   → List (JSON każdego WorkerResultMessage)
///
/// Atomowość:
///   ListRightPushAsync  — dołączenie wyniku do listy jest atomowe
///   IncrementReceivedCountAsync (HINCRBY) — inkrementacja licznika jest atomowa
///   Oba razem NIE są atomowe — dopuszczalne, bo sprawdzamy warunek ukończenia
///   po stronie aplikacji i ewentualne podwójne ustawienie statusu Completed jest idempotentne.
/// </summary>
public sealed class RedisScrapingResultAggregator : IScrapingResultAggregator
{
    private readonly IDatabase _db;
    private readonly ITaskStateStore _stateStore;
    private readonly RedisOptions _options;
    private readonly ILogger<RedisScrapingResultAggregator> _logger;

    public RedisScrapingResultAggregator(
        IConnectionMultiplexer redis,
        ITaskStateStore stateStore,
        IOptions<RedisOptions> options,
        ILogger<RedisScrapingResultAggregator> logger)
    {
        _db = redis.GetDatabase();
        _stateStore = stateStore;
        _options = options.Value;
        _logger = logger;
    }

    public async Task AggregateAsync(string taskId, WorkerResultMessage result, CancellationToken cancellationToken = default)
    {
        var resultsKey = ResultsKey(taskId);

        await _db.ListRightPushAsync(resultsKey, JsonSerializer.Serialize(result));
        await _db.KeyExpireAsync(resultsKey, _options.TaskStateTtl);

        var received = await _stateStore.IncrementReceivedCountAsync(taskId, cancellationToken);
        var expected = await _stateStore.GetExpectedCountAsync(taskId, cancellationToken);

        _logger.LogDebug(
            "Zagregowano wynik od '{Target}' dla zadania {TaskId}. {Received}/{Expected}. Sukces: {Success}",
            result.Target, taskId, received, expected, result.Success);

        if (expected > 0 && received >= expected)
            await FinalizeTaskAsync(taskId, cancellationToken);
    }

    public async Task<AggregatedResult?> GetAggregatedResultAsync(string taskId, CancellationToken cancellationToken = default)
    {
        var status = await _stateStore.GetStatusAsync(taskId, cancellationToken);
        if (status is null)
            return null;

        var results = await FetchAllResultsAsync(taskId);
        return new AggregatedResult(taskId, status.Value, results);
    }

    private async Task FinalizeTaskAsync(string taskId, CancellationToken cancellationToken)
    {
        var results = await FetchAllResultsAsync(taskId);
        var finalStatus = results.Any(r => !r.Success)
            ? OrchestratorTaskStatus.CompletedPartially
            : OrchestratorTaskStatus.Completed;

        await _stateStore.SetStatusAsync(taskId, finalStatus, cancellationToken);

        _logger.LogInformation(
            "Zadanie {TaskId} ukończone ze statusem {Status}. Zebrano {Count} wyników.",
            taskId, finalStatus, results.Count);
    }

    private async Task<List<WorkerResultMessage>> FetchAllResultsAsync(string taskId)
    {
        var raw = await _db.ListRangeAsync(ResultsKey(taskId), 0, -1);

        return raw
            .Where(v => v.HasValue)
            .Select(v =>
            {
                try { return JsonSerializer.Deserialize<WorkerResultMessage>(v!); }
                catch { return null; }
            })
            .Where(r => r is not null)
            .ToList()!;
    }

    private static string ResultsKey(string taskId) => $"task:{taskId}:results";
}
