#!/usr/bin/env bash
set -euo pipefail
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
create() { kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor "${RF:-1}" --config retention.ms="$3" --config min.insync.replicas="${MIN_ISR:-1}"; }
# In production use RF=3, min.insync.replicas=2, producer acks=all.
create commerce.orders 12 604800000
create commerce.commands 12 604800000
create commerce.saga-events 12 1209600000
create commerce.dlq 6 2592000000
create commerce.retry.1m 12 604800000
create commerce.retry.5m 12 604800000
create commerce.retry.15m 12 604800000
create commerce.retry.1h 12 1209600000
create commerce.order-change-enriched.v1 12 1209600000
create analytics.customer-order-windows.v1 12 2592000000
create dbz.public.orders 12 604800000
