-- Run in database: saga. order_payload is a snapshot of the command data needed by later steps.
CREATE TABLE IF NOT EXISTS saga_instances (
  order_id uuid PRIMARY KEY, status text NOT NULL, payment_reference text NULL, reservation_id uuid NULL,
  version integer NOT NULL DEFAULT 0, order_payload jsonb NOT NULL,
  created_at_utc timestamptz NOT NULL, updated_at_utc timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS saga_inbox (
  event_id uuid PRIMARY KEY, payload_hash char(64) NOT NULL, received_at_utc timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS saga_outbox (
  id uuid PRIMARY KEY, aggregate_id uuid NOT NULL, aggregate_version bigint NOT NULL,
  event_type text NOT NULL, payload jsonb NOT NULL,
  created_at_utc timestamptz NOT NULL, published_at_utc timestamptz NULL,
  lock_owner text NULL, locked_until_utc timestamptz NULL
);
ALTER TABLE saga_outbox ADD COLUMN IF NOT EXISTS aggregate_version bigint NULL;
WITH ranked AS (
  SELECT id, row_number() OVER (PARTITION BY aggregate_id ORDER BY created_at_utc,id) AS seq
  FROM saga_outbox WHERE aggregate_version IS NULL
)
UPDATE saga_outbox s SET aggregate_version=ranked.seq FROM ranked WHERE s.id=ranked.id;
ALTER TABLE saga_outbox ALTER COLUMN aggregate_version SET NOT NULL;
CREATE INDEX IF NOT EXISTS ix_saga_outbox_pending ON saga_outbox(aggregate_id,aggregate_version) WHERE published_at_utc IS NULL;
