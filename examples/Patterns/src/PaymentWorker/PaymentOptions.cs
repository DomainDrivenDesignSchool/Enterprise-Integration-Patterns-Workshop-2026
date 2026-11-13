namespace Workshop.PaymentWorker;

public sealed record PaymentOptions(string BootstrapServers, string CommandTopic, string SagaEventsTopic,
    string GroupId, string ConnectionString);
