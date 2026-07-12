#!/usr/bin/env bash
# Pilote Kafka Connect via son API REST (http://localhost:8083). Aucun code.
#
# Usage :
#   ./connect.sh deploy connectors/file-source.json   # cree/met a jour (idempotent)
#   ./connect.sh list                                 # connecteurs actifs
#   ./connect.sh status file-source                   # etat + tasks
#   ./connect.sh config file-source                   # config courante
#   ./connect.sh delete file-source                   # supprime
#   ./connect.sh plugins                              # plugins installes
#   ./connect.sh                                      # resume
set -euo pipefail
cd "$(dirname "$0")"
base="http://localhost:8083"
cmd="${1:-}"; arg="${2:-}"

case "$cmd" in
  list)    curl -s "$base/connectors"; echo ;;
  plugins) curl -s "$base/connector-plugins"; echo ;;
  status)  [ -n "$arg" ] || { echo "Usage : ./connect.sh status <nom>"; exit 1; }; curl -s "$base/connectors/$arg/status"; echo ;;
  config)  [ -n "$arg" ] || { echo "Usage : ./connect.sh config <nom>"; exit 1; }; curl -s "$base/connectors/$arg/config"; echo ;;
  delete)  [ -n "$arg" ] || { echo "Usage : ./connect.sh delete <nom>"; exit 1; }; curl -s -X DELETE "$base/connectors/$arg"; echo "supprime : $arg" ;;
  deploy)
    [ -f "$arg" ] || { echo "Fichier introuvable : $arg"; exit 1; }
    # nom du connecteur lu dans le JSON (pas de dependance a jq)
    name=$(grep -o '"name"[[:space:]]*:[[:space:]]*"[^"]*"' "$arg" | head -1 | sed 's/.*:[[:space:]]*"//; s/"$//')
    # idempotent : on retire l'ancien puis on (re)cree
    curl -s -X DELETE "$base/connectors/$name" >/dev/null 2>&1 || true
    sleep 1
    curl -s -X POST -H "Content-Type: application/json" --data @"$arg" "$base/connectors"; echo
    echo "deploye : $name"
    ;;
  "")
    echo "== GET /connectors =="; curl -s "$base/connectors"; echo ;;
  *) echo "Usage : ./connect.sh <deploy FICHIER|list|status NOM|config NOM|delete NOM|plugins>"; exit 1 ;;
esac
