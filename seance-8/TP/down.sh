#!/usr/bin/env bash
# Arrete toute la pile. Donnees CONSERVEES (volumes + dossier data/).
set -euo pipefail
cd "$(dirname "$0")"
docker compose down
echo "Arrete. Donnees conservees. Tout effacer : ./reset.sh"
