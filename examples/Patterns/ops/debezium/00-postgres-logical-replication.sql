-- PostgreSQL server configuration (postgresql.conf; restart required):
-- wal_level = logical
-- max_replication_slots = 10
-- max_wal_senders = 10
-- max_slot_wal_keep_size = '10GB'   # choose from WAL generation rate and outage RTO
-- wal_sender_timeout = '60s'
--
-- Execute as a privileged DBA in the source cluster:
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='debezium') THEN
    CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'REPLACE_WITH_SECRET';
  END IF;
END $$;
ALTER ROLE debezium WITH REPLICATION;
GRANT CONNECT ON DATABASE orders TO debezium;
\connect orders
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON TABLE public.orders TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;
-- Create publication as DBA; connector is configured with publication.autocreate.mode=disabled.
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname='dbz_orders_publication') THEN
    CREATE PUBLICATION dbz_orders_publication FOR TABLE public.orders;
  END IF;
END $$;
