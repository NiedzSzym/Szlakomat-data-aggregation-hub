using Microsoft.AspNetCore.Mvc;
using TourDataOrchestrator.Application.Abstractions;
using TourDataOrchestrator.Application.DTOs;

namespace TourDataOrchestrator.Api.Controllers;

[ApiController]
[Route("api/providers")]
public sealed class ProvidersController : ControllerBase
{
    private readonly IProviderRegistry _registry;

    public ProvidersController(IProviderRegistry registry)
    {
        _registry = registry;
    }

    /// <summary>
    /// Zwraca listę aktywnych data providerów wraz z obsługiwanymi operacjami i targetami.
    /// Provider jest aktywny dopóki jego TTL w Redis nie wygaśnie (heartbeat co 30s, TTL = 90s).
    /// </summary>
    [HttpGet]
    [ProducesResponseType(typeof(IReadOnlyList<ProviderRegistration>), StatusCodes.Status200OK)]
    public async Task<IActionResult> GetActiveProviders(CancellationToken cancellationToken)
    {
        var providers = await _registry.GetAllActiveAsync(cancellationToken);
        return Ok(providers);
    }
}
