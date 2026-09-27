#!/usr/bin/env bash
set -euo pipefail

SPARK_HOME="${SPARK_HOME:-/usr/local/spark}"
KAFKA_HOST="${KAFKA_HOST:-pd-slave3}"

printf 'Stopping Spark cluster...\n'
"${SPARK_HOME}/sbin/stop-all.sh"

printf 'Stopping ZooKeeper on %s...\n' "$KAFKA_HOST"
ssh "$KAFKA_HOST" sudo -S systemctl stop zookeeper

printf 'Stopping Kafka on %s...\n' "$KAFKA_HOST"
ssh "$KAFKA_HOST" sudo -S systemctl stop kafka

printf 'Cluster services stopped.\n'
