#!/usr/bin/env bash
set -euo pipefail

SPARK_HOME="${SPARK_HOME:-/usr/local/spark}"
KAFKA_HOST="${KAFKA_HOST:-pd-slave3}"

printf 'Starting Spark cluster...\n'
"${SPARK_HOME}/sbin/start-all.sh"

printf 'Starting ZooKeeper on %s...\n' "$KAFKA_HOST"
ssh "$KAFKA_HOST" sudo -S systemctl start zookeeper

printf 'Starting Kafka on %s...\n' "$KAFKA_HOST"
ssh "$KAFKA_HOST" sudo -S systemctl start kafka

printf 'Cluster services started.\n'
