using System.Text.Json.Serialization;

namespace TourDataOrchestrator.Application.DTOs;

public sealed record OrchestratorRequest(
    [property: JsonPropertyName("operation")]    string Operation,
    [property: JsonPropertyName("targets")]      IReadOnlyList<string> Targets,
    [property: JsonPropertyName("parameters")]   RequestParameters Parameters
);

public sealed record RequestParameters(
    [property: JsonPropertyName("date_from")] DateOnly DateFrom,
    [property: JsonPropertyName("pax")]       PaxParameters Pax
);

public sealed record PaxParameters(
    [property: JsonPropertyName("adults")]   int Adults,
    [property: JsonPropertyName("children")] int Children = 0
);
