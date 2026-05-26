using System.Text.Json.Serialization;

namespace TourDataOrchestrator.Application.DTOs;

public sealed record ProviderRegistration(
    [property: JsonPropertyName("provider_id")]       string ProviderId,
    [property: JsonPropertyName("operation")]         string Operation,
    [property: JsonPropertyName("binding_key")]       string BindingKey,
    [property: JsonPropertyName("supported_targets")] IReadOnlyList<string> SupportedTargets,
    [property: JsonPropertyName("description")]       string? Description = null,
    [property: JsonPropertyName("registered_at")]     DateTimeOffset? RegisteredAt = null
);
