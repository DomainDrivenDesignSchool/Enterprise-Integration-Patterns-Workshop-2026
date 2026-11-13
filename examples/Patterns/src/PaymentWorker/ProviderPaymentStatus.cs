namespace Workshop.PaymentWorker;

public sealed record ProviderPaymentStatus(ProviderPaymentState State, string? ProviderReference = null, string? DeclineReason = null);
