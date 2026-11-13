using Workshop.Shared.Sharding;
using Xunit;
public class ShardRouterTests
{
    [Fact] public void SameTenantAlwaysResolvesToSameVirtualBucket()
    {
        var map = BuildMap(); var tenant = Guid.Parse("11111111-1111-1111-1111-111111111111");
        Assert.Equal(map.ResolveVirtualBucket(tenant), map.ResolveVirtualBucket(tenant));
    }

    [Fact] public void ReassigningOneVirtualBucketDoesNotChangeOtherBucketAssignments()
    {
        var original = BuildMap();
        var assignments = Enumerable.Range(0, 64).ToDictionary(i => i, i => new ShardEndpoint(i < 32 ? "shard-a" : "shard-b", $"Host={i}"));
        var tenantByBucket = Enumerable.Range(0, 10000).Select(i => GuidFrom(i)).GroupBy(original.ResolveVirtualBucket).ToDictionary(g => g.Key, g => g.First());
        var movedBucket = tenantByBucket.Keys.First();
        var changed = assignments.ToDictionary(x => x.Key, x => x.Value);
        changed[movedBucket] = new ShardEndpoint("shard-c", "Host=shard-c");
        var remapped = new VirtualBucketShardMap(64, changed);
        foreach (var pair in tenantByBucket)
        {
            var actual = remapped.Resolve(pair.Value).ShardId;
            if (pair.Key == movedBucket) Assert.Equal("shard-c", actual);
            else Assert.Equal(original.Resolve(pair.Value).ShardId, actual);
        }
    }

    private static VirtualBucketShardMap BuildMap() => new(64,
        Enumerable.Range(0, 64).ToDictionary(i => i, i => new ShardEndpoint(i < 32 ? "shard-a" : "shard-b", $"Host={i}")));
    private static Guid GuidFrom(int value) => Guid.Parse(value.ToString("D8") + "-0000-0000-0000-000000000000");
}
