-- Run in database: retry_schedule. Durable delay queue for Kafka retry topics.
CREATE TABLE IF NOT EXISTS scheduled_retries (
  id uuid PRIMARY KEY, original_topic text NOT NULL, original_partition integer NOT NULL,
  source_offset bigint NULL, message_key text NULL, payload bytea NOT NULL, headers_json jsonb NOT NULL,
  attempt integer NOT NULL, due_at_utc timestamptz NOT NULL, created_at_utc timestamptz NOT NULL,
  published_at_utc timestamptz NULL, lock_owner text NULL, locked_until_utc timestamptz NULL
);
CREATE INDEX IF NOT EXISTS ix_scheduled_retries_due ON scheduled_retries(due_at_utc)
  WHERE published_at_utc IS NULL;

ALTER TABLE scheduled_retries ADD COLUMN IF NOT EXISTS source_offset bigint NULL;
CREATE INDEX IF NOT EXISTS ix_scheduled_retries_key_order ON scheduled_retries(original_topic,original_partition,message_key,source_offset) WHERE published_at_utc IS NULL;
