-- Legacy PostgreSQL schema retained only as a reference; current workers use SQL Server.
CREATE TABLE IF NOT EXISTS processed_events (
  consumer_name text NOT NULL,
  event_id uuid NOT NULL,
  payload_hash char(64) NOT NULL,
  processed_at_utc timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_name, event_id)
);
