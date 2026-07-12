#!/usr/bin/env bash
# Affiche les logs des brokers en continu (Ctrl-C pour quitter).
set -euo pipefail
cd "$(dirname "$0")"
docker compose logs -f
