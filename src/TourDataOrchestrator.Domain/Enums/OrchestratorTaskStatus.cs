namespace TourDataOrchestrator.Domain.Enums;

public enum OrchestratorTaskStatus
{
    Processing,
    Completed,
    CompletedPartially,
    Failed,
    TimedOut
}
