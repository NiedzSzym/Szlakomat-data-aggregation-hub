using System.Text.Json.Serialization;

namespace TourDataOrchestrator.Application.DTOs;

public sealed record WorkerTaskMessage(
    [property: JsonPropertyName("task_id")]    string TaskId,
    [property: JsonPropertyName("target")]     string Target,
    [property: JsonPropertyName("operation")]  string Operation,
    [property: JsonPropertyName("parameters")] RequestParameters Parameters,
    [property: JsonPropertyName("reply_to")]   string ReplyToQueue
);

public sealed record WorkerResultMessage(
    [property: JsonPropertyName("task_id")] string TaskId,
    [property: JsonPropertyName("target")]  string Target,
    [property: JsonPropertyName("success")] bool Success,
    [property: JsonPropertyName("payload")] object? Payload,
    [property: JsonPropertyName("error")]   string? Error
);
