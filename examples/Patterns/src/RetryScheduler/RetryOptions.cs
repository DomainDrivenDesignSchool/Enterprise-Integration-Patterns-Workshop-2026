namespace Workshop.RetryScheduler;

public sealed record RetryOptions(string BootstrapServers,string RetryTopics,string GroupId,string ConnectionString);
