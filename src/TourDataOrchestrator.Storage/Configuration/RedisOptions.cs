namespace TourDataOrchestrator.Storage.Configuration;

public sealed class RedisOptions
{
    public const string SectionName = "Redis";

    public string ConnectionString { get; init; } = "localhost:6379";

    /// <summary>
    /// Domyślny TTL dla kluczy stanu zadania — zabezpieczenie przed wyciekiem pamięci
    /// w przypadku błędu workera, który nigdy nie odeśle odpowiedzi.
    /// </summary>
    public TimeSpan TaskStateTtl { get; init; } = TimeSpan.FromMinutes(30);
}
