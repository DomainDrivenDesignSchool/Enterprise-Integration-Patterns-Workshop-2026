namespace EipWorkshop.Saga;

public sealed record SagaStep(
    string Name,
    Func<CancellationToken, Task> Execute,
    Func<CancellationToken, Task> Compensate);

public sealed class SagaOrchestrator
{
    private readonly List<SagaStep> _steps = new();

    public SagaOrchestrator Add(SagaStep step) { _steps.Add(step); return this; }

    public async Task<SagaResult> ExecuteAsync(CancellationToken ct = default)
    {
        var executed = new Stack<SagaStep>();
        try
        {
            foreach (var step in _steps)
            {
                Console.WriteLine($"[Saga] ▶ {step.Name}");
                await step.Execute(ct);
                executed.Push(step);
            }
            return new SagaResult(true, null);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Saga] ✖ failed at: {ex.Message}. Compensating...");
            while (executed.Count > 0)
            {
                var s = executed.Pop();
                try
                {
                    Console.WriteLine($"[Saga] ◀ compensate {s.Name}");
                    await s.Compensate(ct);
                }
                catch (Exception cex)
                {
                    Console.WriteLine($"[Saga] compensation failed for {s.Name}: {cex.Message}");
                }
            }
            return new SagaResult(false, ex);
        }
    }
}

public sealed record SagaResult(bool Success, Exception? Error);