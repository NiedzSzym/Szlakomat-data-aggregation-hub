using Microsoft.AspNetCore.Mvc;
using TourDataOrchestrator.Application.Abstractions;
using TourDataOrchestrator.Application.DTOs;
using TourDataOrchestrator.Domain.Enums;

namespace TourDataOrchestrator.Api.Controllers;

[ApiController]
[Route("api/orchestrator")]
public sealed class OrchestratorController : ControllerBase
{
    private readonly IMessagePublisher _publisher;
    private readonly ITaskStateStore _stateStore;
    private readonly ILogger<OrchestratorController> _logger;

    public OrchestratorController(
        IMessagePublisher publisher,
        ITaskStateStore stateStore,
        ILogger<OrchestratorController> logger)
    {
        _publisher = publisher;
        _stateStore = stateStore;
        _logger = logger;
    }

    /// <summary>
    /// Przyjmuje żądanie orkiestracji. Dla intentu AVAILABILITY zwraca HTTP 202 Accepted
    /// (przetwarzanie asynchroniczne). Dla intentu PRICING może wrócić synchronicznie z cache'u.
    /// </summary>
    [HttpPost("dispatch")]
    [ProducesResponseType(typeof(OrchestratorResponse), StatusCodes.Status202Accepted)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> Dispatch(
        [FromBody] OrchestratorRequest request,
        CancellationToken cancellationToken)
    {
        var taskId = Guid.NewGuid().ToString("N");

        await _stateStore.InitializeAsync(
            taskId,
            expectedWorkerCount: request.Targets.Count,
            ttl: TimeSpan.FromMinutes(30),
            cancellationToken);

        // Fan-out: osobna wiadomość dla każdego targetu (scatter leg Scatter-Gather).
        foreach (var target in request.Targets)
        {
            var routingKey = BuildRoutingKey(target, request.Intent);
            var workerMessage = new WorkerTaskMessage(
                TaskId: taskId,
                Target: target,
                Intent: request.Intent.ToString(),
                Parameters: request.Parameters,
                ReplyToQueue: "orchestrator.results");

            await _publisher.PublishAsync(workerMessage, routingKey, cancellationToken);
        }

        _logger.LogInformation(
            "Rozesłano zadanie {TaskId} do {Count} workerów. Intent: {Intent}",
            taskId, request.Targets.Count, request.Intent);

        return Accepted(new OrchestratorResponse(taskId, OrchestratorTaskStatus.Processing));
    }

    private static string BuildRoutingKey(string target, TaskIntent intent)
    {
        var intentSegment = intent switch
        {
            TaskIntent.Pricing              => "pricing",
            TaskIntent.Availability         => "availability",
            TaskIntent.AvailabilityAndPricing => "full",
            _ => "unknown"
        };
        // Format: "task.{intentSegment}.{target}" → np. "task.full.attraction_wawel"
        return $"task.{intentSegment}.{target}";
    }
}
