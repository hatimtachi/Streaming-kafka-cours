#!/usr/bin/env bash
# Arrete le cluster ET efface toutes les donnees (volumes).
# Pense-bete : Streams stocke aussi un etat local dans /tmp/kafka-streams/<application.id>.
set -euo pipefail
cd "$(dirname "$0")"
docker compose down -v
echo "Cluster arrete et donnees effacees."
echo "Pour repartir d'un etat Streams propre : rm -rf /tmp/kafka-streams/tp9-order-stream"
