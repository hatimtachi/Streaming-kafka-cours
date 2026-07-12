#!/usr/bin/env bash
# Affiche en boucle la repartition des partitions du consumer group de l'application
# Streams (application.id = tp11-currency-aggregator). Lancez une 2e instance et regardez bouger.
# Ctrl-C pour arreter.
set -euo pipefail
cd "$(dirname "$0")"
group="${1:-tp11-currency-aggregator}"
echo "Surveillance du groupe '$group' (Ctrl-C pour arreter)..."
while true; do
  clear 2>/dev/null || true
  echo "== Groupe : $group ==   ($(date +%H:%M:%S))"
  docker compose exec -T broker-1 /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server broker-1:9092 --describe --group "$group" 2>/dev/null \
    || echo "(groupe pas encore actif : lancez ./run.sh app)"
  sleep 3
done
