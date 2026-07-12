#!/usr/bin/env bash
# Arrete tout ET efface les donnees (brokers + schemas enregistres).
set -euo pipefail
cd "$(dirname "$0")"
docker compose down -v
echo "Arrete et donnees effacees (y compris les schemas du registre)."
