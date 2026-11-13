-- Run in database: shipping.
CREATE TABLE IF NOT EXISTS shipments (
  id uuid PRIMARY KEY, order_id uuid NOT NULL UNIQUE, reservation_id uuid NOT NULL,
  status text NOT NULL, created_at_utc timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS shipping_outbox (
  id uuid PRIMARY KEY, order_id uuid NOT NULL, event_type text NOT NULL, payload jsonb NOT NULL,
  created_at_utc timestamptz NOT NULL, published_at_utc timestamptz NULL,
  lock_owner text NULL, locked_until_utc timestamptz NULL
);
CREATE INDEX IF NOT EXISTS ix_shipping_outbox_pending ON shipping_outbox(created_at_utc) WHERE published_at_utc IS NULL;
