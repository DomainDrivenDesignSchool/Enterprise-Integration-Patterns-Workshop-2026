param([string]$Bootstrap = "localhost:9092", [int]$ReplicationFactor = 1)
$topics = @(
  @("commerce.orders",12,604800000), @("commerce.commands",12,604800000),
  @("commerce.saga-events",12,1209600000), @("commerce.dlq",6,2592000000),
  @("commerce.retry.1m",12,604800000), @("commerce.retry.5m",12,604800000),
  @("commerce.retry.15m",12,604800000), @("commerce.retry.1h",12,1209600000),
  @("commerce.order-change-enriched.v1",12,1209600000),
  @("analytics.customer-order-windows.v1",12,2592000000), @("dbz.public.orders",12,604800000)
)
foreach ($t in $topics) {
  kafka-topics --bootstrap-server $Bootstrap --create --if-not-exists --topic $t[0] --partitions $t[1] --replication-factor $ReplicationFactor --config retention.ms=$t[2]
  if ($LASTEXITCODE -ne 0) { throw "Failed to create $($t[0])" }
}
