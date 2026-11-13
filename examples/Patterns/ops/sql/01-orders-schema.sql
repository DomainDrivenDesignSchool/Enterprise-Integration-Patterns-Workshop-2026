-- Run in database: orders. Names are deliberately snake_case because Debezium captures public.orders.
CREATE TABLE IF NOT EXISTS orders (
  id uuid PRIMARY KEY,
  customer_id uuid NOT NULL,
  total_amount numeric(18,2) NOT NULL CHECK (total_amount > 0),
  currency char(3) NOT NULL,
  status text NOT NULL,
  created_at_utc timestamptz NOT NULL,
  version bigint NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS ix_orders_customer_created ON orders(customer_id, created_at_utc DESC);
CREATE TABLE IF NOT EXISTS order_items (
  id uuid PRIMARY KEY,
  order_id uuid NOT NULL REFERENCES orders(id),
  sku text NOT NULL,
  quantity integer NOT NULL CHECK(quantity > 0),
  unit_price numeric(18,2) NOT NULL CHECK(unit_price > 0)
);
CREATE INDEX IF NOT EXISTS ix_order_items_order ON order_items(order_id);
CREATE TABLE IF NOT EXISTS outbox_messages (
  id uuid PRIMARY KEY, aggregate_id uuid NOT NULL, event_type text NOT NULL, payload jsonb NOT NULL,
  occurred_at_utc timestamptz NOT NULL, published_at_utc timestamptz NULL,
  attempts integer NOT NULL DEFAULT 0, next_attempt_at_utc timestamptz NOT NULL,
  locked_until_utc timestamptz NULL, lock_owner text NULL
);
CREATE INDEX IF NOT EXISTS ix_outbox_pending ON outbox_messages(next_attempt_at_utc,occurred_at_utc) WHERE published_at_utc IS NULL;
ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS aggregate_id uuid;
UPDATE outbox_messages
SET aggregate_id = COALESCE(
  NULLIF(payload->'Payload'->>'OrderId','')::uuid,
  NULLIF(payload->'payload'->>'orderId','')::uuid
)
WHERE aggregate_id IS NULL;
ALTER TABLE outbox_messages ALTER COLUMN aggregate_id SET NOT NULL;
CREATE INDEX IF NOT EXISTS ix_outbox_aggregate_order ON outbox_messages(aggregate_id,occurred_at_utc,id) WHERE published_at_utc IS NULL;


-- Required for before-images used to compute paid-amount deltas on UPDATE/DELETE.
-- This increases WAL volume; measure it on high-write tables.
ALTER TABLE orders REPLICA IDENTITY FULL;

CREATE TABLE IF NOT EXISTS order_command_inbox (
  event_id uuid PRIMARY KEY,
  payload_hash char(64) NOT NULL,
  processed_at_utc timestamptz NOT NULL
);
