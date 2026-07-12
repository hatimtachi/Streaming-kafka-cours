#!/usr/bin/env bash
# Arrete le cluster ET efface toutes les donnees (volumes).
set -euo pipefail
cd "$(dirname "$0")"
docker compose down -v
echo "Cluster arrete et donnees effacees."
