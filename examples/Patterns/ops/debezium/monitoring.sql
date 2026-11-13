-- Run on the source PostgreSQL instance as a role allowed to inspect replication slots.
SELECT slot_name, plugin, slot_type, active, restart_lsn, confirmed_flush_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint) AS retained_wal
FROM pg_replication_slots
WHERE slot_name = 'debezium_orders';

SELECT application_name, state, sent_lsn, write_lsn, flush_lsn, replay_lsn,
       write_lag, flush_lag, replay_lag
FROM pg_stat_replication;

-- Alert on inactive slot + growing retained_wal. Do not drop the slot just to clear disk:
-- stopping capture and dropping the slot can force a new initial snapshot and data reprocessing.
