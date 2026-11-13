#!/usr/bin/env bash
# Requires kafka-console-producer and kafka-consumer-groups in PATH.
set -euo pipefail
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
TOPIC="${TOPIC:-commerce.hot-key-lab}"
kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic "$TOPIC" --partitions 12 --replication-factor "${RF:-1}"
echo "1) Hot key: all 10000 records route to the same partition"
seq 1 10000 | sed 's/^/celebrity-customer:/' | kafka-console-producer --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --property parse.key=true --property key.separator=:
echo "2) Distributed keys: 10000 records spread by distinct order keys"
seq 1 10000 | awk '{print "order-" $1 ":" $1}' | kafka-console-producer --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --property parse.key=true --property key.separator=:
echo "3) Inspect partition distribution and consumer lag"
kafka-run-class kafka.tools.GetOffsetShell --broker-list "$BOOTSTRAP" --topic "$TOPIC" --time -1
kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" --all-groups --describe || true
