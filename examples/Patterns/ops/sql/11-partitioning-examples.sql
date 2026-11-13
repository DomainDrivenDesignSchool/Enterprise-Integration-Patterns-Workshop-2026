-- Run in an isolated demo database. PostgreSQL partitioning is within one cluster; it is not sharding.
CREATE TABLE IF NOT EXISTS order_audit (
  event_id uuid NOT NULL,
  order_id uuid NOT NULL,
  event_time_utc timestamptz NOT NULL,
  event_type text NOT NULL,
  payload jsonb NOT NULL,
  PRIMARY KEY(event_id,event_time_utc)
) PARTITION BY RANGE(event_time_utc);
CREATE TABLE IF NOT EXISTS order_audit_2026_11 PARTITION OF order_audit
  FOR VALUES FROM ('2026-11-01 00:00:00+00') TO ('2026-12-01 00:00:00+00');
CREATE INDEX IF NOT EXISTS ix_order_audit_order_time ON order_audit(order_id,event_time_utc DESC);
-- Demonstrate pruning:
EXPLAIN (ANALYZE,BUFFERS)
SELECT * FROM order_audit
WHERE event_time_utc >= '2026-11-01' AND event_time_utc < '2026-12-01'
  AND order_id='aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';

-- Hot-row contention demo: run this UPDATE concurrently from many sessions.
-- The conditional update is correct, but one popular SKU serializes on one row.
UPDATE stock_items SET available=available-1,reserved=reserved+1
WHERE sku='SKU-KEYBOARD' AND available>=1;
-- Inspect waiting sessions/locks while running concurrent clients:
SELECT pid,wait_event_type,wait_event,state,query
FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock';
SELECT locktype,relation::regclass,mode,granted,count(*)
FROM pg_locks WHERE relation IS NOT NULL GROUP BY locktype,relation,mode,granted ORDER BY count(*) DESC;
