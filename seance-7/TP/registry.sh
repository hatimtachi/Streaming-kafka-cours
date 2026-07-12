#!/usr/bin/env bash
# Interroge le Schema Registry (REST sur localhost:8081).
#
# Sans argument : resume (sujets, versions de orders-value, mode de compatibilite).
# Avec un chemin : ./registry.sh subjects/orders-value/versions/2
set -euo pipefail
cd "$(dirname "$0")"
base="http://localhost:8081"

if [ "$#" -eq 0 ]; then
  echo "== GET /subjects =="
  curl -s "$base/subjects"; echo
  echo "== GET /subjects/orders-value/versions =="
  curl -s "$base/subjects/orders-value/versions"; echo
  echo "== GET /config/orders-value  (mode de compatibilite) =="
  curl -s "$base/config/orders-value"; echo
else
  curl -s "$base/$1"; echo
fi
