using System.Text.Json.Serialization;
using TourDataOrchestrator.Domain.Enums;

namespace TourDataOrchestrator.Application.DTOs;

public sealed record OrchestratorRequest(
    [property: JsonPropertyName("intent")]   TaskIntent Intent,
    [property: JsonPropertyName("targets")]  IReadOnlyList<string> Targets,
    [property: JsonPropertyName("parameters")] RequestParameters Parameters
);

public sealed record RequestParameters(
    [property: JsonPropertyName("date_from")] DateOnly DateFrom,
    [property: JsonPropertyName("pax")]       PaxParameters Pax
);

public sealed record PaxParameters(
    [property: JsonPropertyName("adults")]   int Adults,
    [property: JsonPropertyName("children")] int Children = 0
);
