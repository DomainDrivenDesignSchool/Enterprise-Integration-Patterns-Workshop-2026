-- Run in database: analytics. This is a rebuildable read model, not the financial source of truth.
CREATE TABLE IF NOT EXISTS analytics_inbox(event_id uuid PRIMARY KEY,payload_hash char(64) NOT NULL,processed_at_utc timestamptz NOT NULL);
CREATE TABLE IF NOT EXISTS customer_order_windows(
 customer_key text NOT NULL,currency char(3) NOT NULL,window_start_utc timestamptz NOT NULL,window_end_utc timestamptz NOT NULL,
 change_count bigint NOT NULL,net_paid_amount numeric(18,2) NOT NULL,last_event_id uuid NOT NULL,updated_at_utc timestamptz NOT NULL,
 PRIMARY KEY(customer_key,currency,window_start_utc)
);
CREATE INDEX IF NOT EXISTS ix_customer_order_windows_end ON customer_order_windows(window_end_utc DESC);
