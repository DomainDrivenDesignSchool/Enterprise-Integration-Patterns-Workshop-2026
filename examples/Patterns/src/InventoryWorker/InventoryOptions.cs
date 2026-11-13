namespace Workshop.InventoryWorker;

public sealed record InventoryOptions(string BootstrapServers, string CommandTopic, string SagaEventsTopic,
    string GroupId, string ConnectionString);
