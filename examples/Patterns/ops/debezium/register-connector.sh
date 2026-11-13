#!/usr/bin/env bash
set -euo pipefail
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
curl --fail --show-error -i -X PUT "$CONNECT_URL/connectors/orders-postgres-cdc/config" \
  -H 'Content-Type: application/json' \
  --data-binary @"$(dirname "$0")/postgres-orders-connector.json"
printf '\nConnector status:\n'
curl --fail --show-error "$CONNECT_URL/connectors/orders-postgres-cdc/status"
