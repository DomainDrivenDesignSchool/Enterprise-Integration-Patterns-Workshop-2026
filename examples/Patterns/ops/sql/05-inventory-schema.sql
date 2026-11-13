-- Run in database: inventory. Conditional UPDATE prevents negative stock under concurrent orders.
CREATE TABLE IF NOT EXISTS stock_items (
  sku text PRIMARY KEY, available integer NOT NULL CHECK(available >= 0),
  reserved integer NOT NULL DEFAULT 0 CHECK(reserved >= 0), version bigint NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS stock_reservations (
  id uuid PRIMARY KEY, order_id uuid NOT NULL UNIQUE, status text NOT NULL CHECK(status IN ('Reserving','Reserved','Released','Rejected')),
  request_hash text NULL,
  created_at_utc timestamptz NOT NULL, released_at_utc timestamptz NULL
);
ALTER TABLE stock_reservations ADD COLUMN IF NOT EXISTS request_hash text NULL;
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname='stock_reservations_request_hash_check' AND conrelid='stock_reservations'::regclass) THEN
    ALTER TABLE stock_reservations DROP CONSTRAINT stock_reservations_request_hash_check;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='ck_stock_reservations_request_hash' AND conrelid='stock_reservations'::regclass) THEN
    ALTER TABLE stock_reservations ADD CONSTRAINT ck_stock_reservations_request_hash
      CHECK(request_hash IS NULL OR length(request_hash)=64);
  END IF;
  IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname='stock_reservations_status_check' AND conrelid='stock_reservations'::regclass) THEN
    ALTER TABLE stock_reservations DROP CONSTRAINT stock_reservations_status_check;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='ck_stock_reservations_status' AND conrelid='stock_reservations'::regclass) THEN
    ALTER TABLE stock_reservations ADD CONSTRAINT ck_stock_reservations_status
      CHECK(status IN ('Reserving','Reserved','Released','Rejected'));
  END IF;
END $$;
CREATE TABLE IF NOT EXISTS stock_reservation_lines (
  reservation_id uuid NOT NULL REFERENCES stock_reservations(id), sku text NOT NULL REFERENCES stock_items(sku),
  quantity integer NOT NULL CHECK(quantity > 0), PRIMARY KEY(reservation_id,sku)
);
CREATE TABLE IF NOT EXISTS inventory_outbox (
  id uuid PRIMARY KEY, aggregate_id uuid NOT NULL, event_type text NOT NULL, payload jsonb NOT NULL,
  created_at_utc timestamptz NOT NULL, published_at_utc timestamptz NULL,
  lock_owner text NULL, locked_until_utc timestamptz NULL
);
CREATE INDEX IF NOT EXISTS ix_inventory_outbox_pending ON inventory_outbox(created_at_utc) WHERE published_at_utc IS NULL;
INSERT INTO stock_items(sku,available,reserved) VALUES ('SKU-KEYBOARD',100,0),('SKU-MOUSE',250,0),('SKU-MONITOR',40,0)
ON CONFLICT(sku) DO NOTHING;
