#!/usr/bin/env bash
set -euo pipefail
: "${CONNECT_URL:=http://localhost:8083}"
CONFIG="$(dirname "$0")/sqlserver-orders-connector.example.json"
# POST accepts the connector wrapper {name, config}. For updates to an existing connector,
# use PUT /connectors/orders-sqlserver-cdc/config with only the inner .config object.
curl --fail --show-error -i -X POST "$CONNECT_URL/connectors" \
  -H 'Content-Type: application/json' --data-binary "@$CONFIG"
curl --fail --show-error "$CONNECT_URL/connectors/orders-sqlserver-cdc/status"
