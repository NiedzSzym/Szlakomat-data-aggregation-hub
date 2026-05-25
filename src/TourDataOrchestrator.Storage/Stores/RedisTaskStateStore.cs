using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using TourDataOrchestrator.Application.Abstractions;
using TourDataOrchestrator.Domain.Enums;
using TourDataOrchestrator.Storage.Configuration;

namespace TourDataOrchestrator.Storage.Stores;

/// <summary>
/// Implementacja <see cref="ITaskStateStore"/> oparta na Redis Hash.
///
/// Schemat klucza: "task:{taskId}" (Hash)
///   - field "expected"  → liczba workerów oczekiwanych
///   - field "received"  → liczba odebranych odpowiedzi (inkrementowana atomowo przez HashIncrementAsync)
///   - field "status"    → wartość enum <see cref="OrchestratorTaskStatus"/>
///
/// Atomowość HINCRBY (HashIncrementAsync) eliminuje Race Condition przy równoległych
/// odpowiedziach wielu workerów — Redis jest jednowątkowy w zakresie poleceń.
/// </summary>
public sealed class RedisTaskStateStore : ITaskStateStore
{
    private readonly IDatabase _db;
    private readonly RedisOptions _options;
    private readonly ILogger<RedisTaskStateStore> _logger;

    private const string FieldExpected = "expected";
    private const string FieldReceived = "received";
    private const string FieldStatus   = "status";

    public RedisTaskStateStore(
        IConnectionMultiplexer redis,
        IOptions<RedisOptions> options,
        ILogger<RedisTaskStateStore> logger)
    {
        _db = redis.GetDatabase();
        _options = options.Value;
        _logger = logger;
    }

    public async Task InitializeAsync(
        string taskId, int expectedWorkerCount, TimeSpan ttl,
        CancellationToken cancellationToken = default)
    {
        var key = BuildKey(taskId);

        // Transakcja lub pipeline: wszystkie trzy pola ustawiamy atomowo, a następnie ustawiamy TTL.
        var batch = _db.CreateBatch();
        var setExpected = batch.HashSetAsync(key, FieldExpected, expectedWorkerCount);
        var setReceived = batch.HashSetAsync(key, FieldReceived, 0);
        var setStatus   = batch.HashSetAsync(key, FieldStatus,   OrchestratorTaskStatus.Processing.ToString());
        batch.Execute();

        await Task.WhenAll(setExpected, setReceived, setStatus);

        // TTL jako zabezpieczenie przed wyciekiem — klucz wygaśnie nawet jeśli task nie ukończy się normalnie.
        await _db.KeyExpireAsync(key, ttl);

        _logger.LogDebug("Zainicjowano stan zadania {TaskId}: {Expected} workerów oczekiwanych.", taskId, expectedWorkerCount);
    }

    /// <summary>
    /// Atomowa inkrementacja (HINCRBY) — bezpieczna przy concurrent access wielu goroutines/wątków.
    /// Zwraca nową wartość licznika po inkrementacji.
    /// </summary>
    public Task<long> IncrementReceivedCountAsync(string taskId, CancellationToken cancellationToken = default)
        => _db.HashIncrementAsync(BuildKey(taskId), FieldReceived);

    public async Task<int> GetExpectedCountAsync(string taskId, CancellationToken cancellationToken = default)
    {
        var value = await _db.HashGetAsync(BuildKey(taskId), FieldExpected);
        return value.HasValue ? (int)value : 0;
    }

    public Task SetStatusAsync(string taskId, OrchestratorTaskStatus status, CancellationToken cancellationToken = default)
        => _db.HashSetAsync(BuildKey(taskId), FieldStatus, status.ToString());

    public async Task<OrchestratorTaskStatus?> GetStatusAsync(string taskId, CancellationToken cancellationToken = default)
    {
        var value = await _db.HashGetAsync(BuildKey(taskId), FieldStatus);
        if (!value.HasValue) return null;
        return Enum.TryParse<OrchestratorTaskStatus>(value!, out var status) ? status : null;
    }

    private static string BuildKey(string taskId) => $"task:{taskId}";
}
