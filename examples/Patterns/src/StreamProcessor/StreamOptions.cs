namespace Workshop.StreamProcessor;

public sealed record StreamOptions(string BootstrapServers, string InputTopic, string OutputTopic, string GroupId, string ConnectionString);
