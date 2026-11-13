namespace Workshop.Shared.Sharding;

public interface IShardMap
{
    ShardEndpoint Resolve(Guid tenantId);
    int ResolveVirtualBucket(Guid tenantId);
}
