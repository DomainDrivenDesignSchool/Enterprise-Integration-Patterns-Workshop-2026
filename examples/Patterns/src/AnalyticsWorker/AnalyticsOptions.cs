namespace Workshop.AnalyticsWorker;

public sealed record AnalyticsOptions(string BootstrapServers,string InputTopic,string GroupId,string ConnectionString);
