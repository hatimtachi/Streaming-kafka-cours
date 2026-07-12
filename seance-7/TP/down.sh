#!/usr/bin/env bash
# Arrete le cluster + le registre. Les donnees sont CONSERVEES (volumes).
set -euo pipefail
cd "$(dirname "$0")"
docker compose down
echo "Arrete. Donnees conservees. Tout effacer : ./reset.sh"
