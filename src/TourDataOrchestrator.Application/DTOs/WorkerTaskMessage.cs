using System.Text.Json.Serialization;

namespace TourDataOrchestrator.Application.DTOs;

/// <summary>
/// Message published to the exchange for each target worker (fan-out leg of Scatter-Gather).
/// </summary>
public sealed record WorkerTaskMessage(
    [property: JsonPropertyName("task_id")]      string TaskId,
    [property: JsonPropertyName("target")]       string Target,
    [property: JsonPropertyName("intent")]       string Intent,
    [property: JsonPropertyName("parameters")]   RequestParameters Parameters,
    [property: JsonPropertyName("reply_to")]     string ReplyToQueue
);

/// <summary>
/// Message received on orchestrator.results (gather leg of Scatter-Gather).
/// </summary>
public sealed record WorkerResultMessage(
    [property: JsonPropertyName("task_id")]  string TaskId,
    [property: JsonPropertyName("target")]   string Target,
    [property: JsonPropertyName("success")]  bool Success,
    [property: JsonPropertyName("payload")]  object? Payload,
    [property: JsonPropertyName("error")]    string? Error
);
