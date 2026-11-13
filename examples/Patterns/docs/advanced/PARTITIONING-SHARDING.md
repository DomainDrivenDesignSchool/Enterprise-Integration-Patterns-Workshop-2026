# Partitioning and Sharding in Application Design

## Three different concepts

- Kafka partition: the unit of ordering and parallelism within a topic; in a consumer group, each partition is assigned to one consumer at a time.
- SQL Server table partitioning: a physical/logical split of a large table within one database, for example by month; by itself it is not scale-out or distribution across multiple nodes.
- Application sharding: dividing data among several independent databases; the application must determine the destination of queries and commands.

## Choosing the key

In the order topic, `orderId` is a suitable key because it preserves the ordering of the events of one order and spreads load across orders. For the main order topic, `customerId` might turn a high-traffic customer into a hot partition. If analytics needs per-customer ordering/aggregation, after consuming the main topic build an analytics topic with key=`customerId`.

For tenant-based sharding, `tenantId` is usually a suitable shard key if most operations stay within a tenant. The key must:
- be accessible in all the queries/commands of the aggregate;
- have suitable cardinality and distribution;
- not be mutable;
- be aligned with locality and privacy needs.

## Routing Queries and Commands

In the repository, `src/Shared/Sharding/VirtualBucketShardRouter.cs` implements this pattern with virtual buckets. `VirtualBucketShardMap.Resolve(tenantId)` returns the deterministic endpoint, and `TenantCommandRouter<TSession>` sends both Queries and Commands to that same endpoint. Implement `IShardSessionFactory<TSession>` with the real DbContext/connection of each shard; Connection Strings must come from a secret store.

Queries that carry a tenantId are routed to one shard. Global queries must use a global read model, a search index, or a bounded fan-out. For fan-out you need: bounded parallelism, a per-shard timeout, a partial-result policy, a global pagination cursor and an ordered merge. Never silently broadcast a query without a tenant filter to all shards.

## Right and wrong design

| Wrong | Why it is wrong | Fix |
|---|---|---|
| `hash(orderId) % shardCount` with a fixed shard count | Increasing shardCount moves all mappings | virtual buckets + a changeable shard map, or consistent/rendezvous hashing |
| Picking a shard at random on each request | Reads and writes of one entity go to different DBs | deterministic routing based on the shard key |
| Joins across shards in OLTP | Latency, fan-out and complicated failure modes | denormalized read model / CQRS projection |
| A transaction across two shards for one command | Requires a distributed transaction and coupling | Fix the aggregate boundary; make the cross-shard workflow a Saga |
| A low-cardinality shard key such as status or date | Hot shard and skew | tenant/customer/aggregate key with a measured distribution |
| Increasing partitions without checking key skew | The hot key still stays in one partition | Fix the key/event model or build a two-stage aggregation |

## Hot partition in Kafka

If 70% of messages have the same key, adding consumers or partitions does not necessarily increase that key's throughput; one key goes to one partition. To measure, look at bytes/messages per partition, consumer lag per partition, produce request latency and key frequency.

If ordering of the whole aggregate is mandatory, keep that aggregate key and raise throughput by increasing the capacity of the partition leader and reducing per-message work. If a very high-traffic customer causes a hot key in analytics and full per-customer ordering is not needed, use `customerId + bucket(orderId)` followed by a second-stage aggregation. This salting destroys the overall customer ordering; it is allowed only when the semantics permit.

## SQL Server partitioning

For an append-heavy table such as audit/events, build a monthly RANGE partition on `created_at_utc`; queries must have a time predicate so that partition pruning occurs. In SQL Server, design aligned indexes and the partition scheme carefully; for a unique index on a partitioned table, the partition key must be included in the unique key. Check the restrictions for FKs and partition SWITCH before designing. A runnable sample is in `ops/sqlserver/11-partitioning-examples.sql`; it places all partitions on `[PRIMARY]` so it can run independently of extra filegroups. In Production, evaluate separate filegroups and aligned indexes for I/O, retention and maintenance. Partitioning does not replace indexes and query plans.

## Hot row / hot SKU

`stock_items` with a very popular SKU becomes a hot row; lock contention on that row is natural. Options worth evaluating: reservation buckets (several stock buckets), distributing stock per warehouse, optimistic versioning, a queue per SKU, or limiting concurrency. Never decrement stock without an atomic condition; a separate SELECT followed by UPDATE creates a race.
