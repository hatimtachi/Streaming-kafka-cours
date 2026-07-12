#!/usr/bin/env bash
# Arrete le cluster. Les donnees des topics sont CONSERVEES (volumes).
set -euo pipefail
cd "$(dirname "$0")"
docker compose down
echo "Cluster arrete. Donnees conservees. Tout effacer : ./reset.sh"
