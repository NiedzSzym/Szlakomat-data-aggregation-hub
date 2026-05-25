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
    private readonly IScrapingResultAggregator _aggregator;
    private readonly ILogger<OrchestratorController> _logger;

    public OrchestratorController(
        IMessagePublisher publisher,
        ITaskStateStore stateStore,
        IScrapingResultAggregator aggregator,
        ILogger<OrchestratorController> logger)
    {
        _publisher = publisher;
        _stateStore = stateStore;
        _aggregator = aggregator;
        _logger = logger;
    }

    /// <summary>
    /// Scatter leg: rozsyła zadania do workerów. Zawsze zwraca HTTP 202 z task_id.
    /// Klient powinien pollować GET /{taskId} aż status != Processing.
    /// </summary>
    [HttpPost("dispatch")]
    [ProducesResponseType(typeof(OrchestratorResponse), StatusCodes.Status202Accepted)]
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

    /// <summary>
    /// Gather leg: zwraca zagregowany wynik zadania.
    /// HTTP 202 → nadal przetwarza; HTTP 200 → Completed / CompletedPartially; HTTP 404 → nieznany lub wygasły task_id.
    /// </summary>
    [HttpGet("{taskId}")]
    [ProducesResponseType(typeof(AggregatedResult), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(AggregatedResult), StatusCodes.Status202Accepted)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetResult(string taskId, CancellationToken cancellationToken)
    {
        var result = await _aggregator.GetAggregatedResultAsync(taskId, cancellationToken);

        if (result is null)
            return NotFound(new { task_id = taskId, error = "Nieznane zadanie lub TTL wygasł." });

        return result.Status == OrchestratorTaskStatus.Processing
            ? Accepted(result)
            : Ok(result);
    }

    private static string BuildRoutingKey(string target, TaskIntent intent)
    {
        var segment = intent switch
        {
            TaskIntent.Pricing                => "pricing",
            TaskIntent.Availability           => "availability",
            TaskIntent.AvailabilityAndPricing => "full",
            _                                 => "unknown"
        };
        return $"task.{segment}.{target}";
    }
}
