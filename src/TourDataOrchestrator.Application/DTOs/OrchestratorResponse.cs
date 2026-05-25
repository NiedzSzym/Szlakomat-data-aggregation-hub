using System.Text.Json.Serialization;
using TourDataOrchestrator.Domain.Enums;

namespace TourDataOrchestrator.Application.DTOs;

public sealed record OrchestratorResponse(
    [property: JsonPropertyName("task_id")] string TaskId,
    [property: JsonPropertyName("status")]  OrchestratorTaskStatus Status
);

public sealed record AggregatedResult(
    string TaskId,
    OrchestratorTaskStatus Status,
    IReadOnlyList<WorkerResultMessage> Results
);
