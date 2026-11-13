namespace Workshop.Shared.Events;
public sealed record OrderFailed(Guid OrderId, string Reason);
