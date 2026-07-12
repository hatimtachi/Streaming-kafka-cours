#!/usr/bin/env bash
# Lit le topic de sortie "orders-validated" avec un niveau d'isolation donne.
#
# Usage : ./read-validated.sh [read_committed|read_uncommitted]   (defaut : read_committed)
#   read_committed   : ne voit QUE les transactions validees (le poison n'apparait pas)
#   read_uncommitted : voit tout, y compris les messages des transactions abandonnees
set -euo pipefail
cd "$(dirname "$0")"
level="${1:-read_committed}"

echo "Lecture de 'orders-validated' en $level (timeout 4s)..."
docker compose exec -T broker-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server broker-1:9092 --topic orders-validated --from-beginning \
  --isolation-level "$level" --timeout-ms 4000 --property print.key=true || true
