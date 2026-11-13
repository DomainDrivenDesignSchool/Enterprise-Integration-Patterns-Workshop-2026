namespace Workshop.Shared.Sharding;

public sealed record ShardEndpoint(string ShardId, string ConnectionString);
