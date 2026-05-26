using TourDataOrchestrator.Application.DTOs;

namespace TourDataOrchestrator.Application.Abstractions;

public interface IProviderRegistry
{
    Task RegisterAsync(ProviderRegistration registration, TimeSpan ttl, CancellationToken cancellationToken = default);
    Task RefreshHeartbeatAsync(string providerId, TimeSpan ttl, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<ProviderRegistration>> GetAllActiveAsync(CancellationToken cancellationToken = default);
}
