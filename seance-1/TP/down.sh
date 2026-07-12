#!/usr/bin/env bash
# Arrete le cluster. Les donnees des topics sont CONSERVEES (volume kafka-data).
set -euo pipefail
cd "$(dirname "$0")"

docker compose down

echo "Cluster arrete. Les donnees sont conservees."
echo "Pour tout effacer et repartir a neuf : ./reset.sh"
