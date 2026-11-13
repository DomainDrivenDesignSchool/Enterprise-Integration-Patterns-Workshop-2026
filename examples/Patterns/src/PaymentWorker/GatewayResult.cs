namespace Workshop.PaymentWorker;

public sealed record GatewayResult(bool Approved, string? ProviderReference, string? DeclineReason);
