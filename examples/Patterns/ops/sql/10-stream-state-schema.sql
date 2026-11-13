-- Run in database: streamstate. State and inbox are durable across restarts and consumer rebalances.
CREATE TABLE IF NOT EXISTS stream_inbox (
  event_id uuid PRIMARY KEY, topic text NOT NULL, partition_id integer NOT NULL,
  offset_value bigint NOT NULL, payload_hash char(64) NOT NULL, processed_at_utc timestamptz NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_stream_inbox_position ON stream_inbox(topic,partition_id,offset_value);
-- Per-customer watermark: the input topic may be partitioned by orderId, while
-- multiple orders for the same customer can land on different Kafka partitions.
CREATE TABLE IF NOT EXISTS stream_key_state (
  topic text NOT NULL, customer_key text NOT NULL, currency char(3) NOT NULL, max_event_time_utc timestamptz NOT NULL,
  PRIMARY KEY(topic,customer_key,currency)
);
CREATE TABLE IF NOT EXISTS stream_window_state (
  customer_key text NOT NULL, currency char(3) NOT NULL, window_start_utc timestamptz NOT NULL, window_end_utc timestamptz NOT NULL,
  event_count bigint NOT NULL, total_amount numeric(18,2) NOT NULL,
  last_event_time_utc timestamptz NOT NULL, emitted boolean NOT NULL DEFAULT false,
  PRIMARY KEY(customer_key,currency,window_start_utc)
);
CREATE TABLE IF NOT EXISTS stream_late_events (
  event_id uuid PRIMARY KEY, customer_key text NOT NULL, currency char(3) NOT NULL, event_time_utc timestamptz NOT NULL,
  amount numeric(18,2) NOT NULL, reason text NOT NULL, recorded_at_utc timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS stream_outbox (
  id uuid PRIMARY KEY, customer_key text NOT NULL, currency char(3) NOT NULL, payload jsonb NOT NULL,
  created_at_utc timestamptz NOT NULL, published_at_utc timestamptz NULL,
  lock_owner text NULL, locked_until_utc timestamptz NULL
);
CREATE INDEX IF NOT EXISTS ix_stream_outbox_pending ON stream_outbox(created_at_utc) WHERE published_at_utc IS NULL;
