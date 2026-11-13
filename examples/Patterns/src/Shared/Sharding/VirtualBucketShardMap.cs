using System.Security.Cryptography;
using System.Text;

namespace Workshop.Shared.Sharding;

/// <summary>
/// Stable tenant routing through virtual buckets. Moving a bucket changes only that bucket's
/// assignment; the hash function and tenant identity remain unchanged.
/// Load assignments from a versioned control-plane table/config, not hard-coded in services.
/// </summary>
public sealed class VirtualBucketShardMap : IShardMap
{
    private readonly int _bucketCount;
    private readonly IReadOnlyDictionary<int, ShardEndpoint> _assignments;

    public VirtualBucketShardMap(int bucketCount, IReadOnlyDictionary<int, ShardEndpoint> assignments)
    {
        if (bucketCount <= 0) throw new ArgumentOutOfRangeException(nameof(bucketCount));
        if (assignments.Count != bucketCount || Enumerable.Range(0, bucketCount).Any(x => !assignments.ContainsKey(x)))
            throw new ArgumentException("Every virtual bucket must have exactly one shard assignment.", nameof(assignments));
        _bucketCount = bucketCount;
        _assignments = assignments;
    }

    public int ResolveVirtualBucket(Guid tenantId)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(tenantId.ToString("N")));
        var hash = System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(bytes);
        return (int)(hash % (uint)_bucketCount);
    }

    public ShardEndpoint Resolve(Guid tenantId) => _assignments[ResolveVirtualBucket(tenantId)];
}
