#!/usr/bin/env bash
set -euo pipefail
: "${PGHOST:=localhost}"; : "${PGPORT:=5432}"; : "${PGUSER:=postgres}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
psql -v ON_ERROR_STOP=1 -f "$ROOT/ops/sql/00-create-databases.sql"
run(){ local db="$1" file="$2"; PGDATABASE="$db" psql -v ON_ERROR_STOP=1 -f "$ROOT/ops/sql/$file"; }
run orders 01-orders-schema.sql
run payment 04-payment-schema.sql
run inventory 05-inventory-schema.sql
run saga 06-saga-schema.sql
run shipping 07-shipping-schema.sql
run integration_read 08-cdc-enrichment-schema.sql
run retry_schedule 09-retry-schedule-schema.sql
run streamstate 10-stream-state-schema.sql

run analytics 12-analytics-schema.sql
