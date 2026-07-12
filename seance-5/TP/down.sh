#!/usr/bin/env bash
# Arrete le cluster. Les donnees sont CONSERVEES (volumes).
set -euo pipefail
cd "$(dirname "$0")"
docker compose down
echo "Cluster arrete. Donnees conservees. Tout effacer : ./reset.sh"
