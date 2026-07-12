#!/usr/bin/env bash
# Arrete le cluster ET efface toutes les donnees (volume). Repart de zero.
set -euo pipefail
cd "$(dirname "$0")"

docker compose down -v

echo "Cluster arrete et donnees effacees."
echo "'./up.sh' (ou 'docker compose up -d') repartira sur un cluster vierge."
