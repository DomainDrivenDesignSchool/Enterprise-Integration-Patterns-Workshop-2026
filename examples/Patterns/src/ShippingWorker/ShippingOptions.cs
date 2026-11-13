namespace Workshop.ShippingWorker;

public sealed record ShippingOptions(string BootstrapServers,string CommandTopic,string SagaEventsTopic,string GroupId,string ConnectionString);
