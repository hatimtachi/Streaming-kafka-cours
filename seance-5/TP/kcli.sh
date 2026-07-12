#!/usr/bin/env bash
# Lance un outil CLI Kafka dans le conteneur broker-1 (adresse interne broker-1:9092).
# Exemple : ./kcli.sh kafka-topics.sh --bootstrap-server broker-1:9092 --create --topic events --partitions 3 --replication-factor 3
set -euo pipefail
cd "$(dirname "$0")"
if [ "$#" -eq 0 ]; then
  echo "Usage : ./kcli.sh <outil-kafka.sh> [arguments...]" >&2
  exit 1
fi
tool="$1"; shift
exec docker compose exec broker-1 /opt/kafka/bin/"$tool" "$@"
