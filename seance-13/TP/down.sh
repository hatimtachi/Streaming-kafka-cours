#!/usr/bin/env bash
# Arrete le cluster.  Ajoutez l'argument "clean" pour effacer aussi l'etat (volumes).
set -euo pipefail
cd "$(dirname "$0")"
if [ "${1:-}" = "clean" ]; then
  echo "== Cluster : arret + purge des volumes (etat efface) =="
  docker compose down -v
else
  echo "== Cluster : arret (etat conserve) =="
  docker compose down
fi
