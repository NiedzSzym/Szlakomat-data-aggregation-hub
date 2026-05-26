using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System.Text.Json;
using TourDataOrchestrator.Application.Abstractions;
using TourDataOrchestrator.Application.DTOs;

namespace TourDataOrchestrator.Storage.Stores;

/// <summary>
/// Schemat kluczy Redis:
///   provider:{providerId}  → JSON string (ProviderRegistration), TTL = heartbeat TTL
///   providers:index        → Set zawierający wszystkie zarejestrowane provider IDs
///
/// Heartbeat: worker co N sekund wywołuje RefreshHeartbeatAsync, który odnawia TTL klucza.
/// Po wygaśnięciu TTL klucz znika — provider jest nieaktywny. Set providers:index
/// jest czyszczony lazy przy odczycie (pola bez aktywnego klucza są pomijane).
/// </summary>
public sealed class RedisProviderRegistry : IProviderRegistry
{
    private const string IndexKey = "providers:index";

    private static string ProviderKey(string providerId) => $"provider:{providerId}";

    private readonly IDatabase _db;
    private readonly ILogger<RedisProviderRegistry> _logger;

    public RedisProviderRegistry(IConnectionMultiplexer multiplexer, ILogger<RedisProviderRegistry> logger)
    {
        _db = multiplexer.GetDatabase();
        _logger = logger;
    }

    public async Task RegisterAsync(ProviderRegistration registration, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        var stamped = registration with { RegisteredAt = DateTimeOffset.UtcNow };
        var json = JsonSerializer.Serialize(stamped);

        await _db.StringSetAsync(ProviderKey(registration.ProviderId), json, ttl);
        await _db.SetAddAsync(IndexKey, registration.ProviderId);

        _logger.LogInformation(
            "Provider '{ProviderId}' zarejestrowany. Operacja: '{Operation}', Targety: [{Targets}], TTL: {Ttl}s",
            registration.ProviderId, registration.Operation,
            string.Join(", ", registration.SupportedTargets),
            (int)ttl.TotalSeconds);
    }

    public Task RefreshHeartbeatAsync(string providerId, TimeSpan ttl, CancellationToken cancellationToken = default)
        => _db.KeyExpireAsync(ProviderKey(providerId), ttl);

    public async Task<IReadOnlyList<ProviderRegistration>> GetAllActiveAsync(CancellationToken cancellationToken = default)
    {
        var members = await _db.SetMembersAsync(IndexKey);
        if (members.Length == 0) return [];

        // MGET zamiast N pojedynczych GETów — jeden round-trip do Redis.
        var keys = members.Select(m => (RedisKey)ProviderKey((string)m!)).ToArray();
        var values = await _db.StringGetAsync(keys);

        var result = new List<ProviderRegistration>(values.Length);
        foreach (var value in values)
        {
            if (value.IsNull) continue;
            var reg = JsonSerializer.Deserialize<ProviderRegistration>(value!);
            if (reg is not null)
                result.Add(reg);
        }

        return result;
    }
}
