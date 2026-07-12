#!/usr/bin/env bash
# Demarre le cluster Kafka (1 broker KRaft) + Kafka UI.
set -euo pipefail
cd "$(dirname "$0")"

docker compose up -d

echo
echo "Cluster en cours de demarrage..."
echo "  - Broker (CLI) : localhost:9092"
echo "  - Kafka UI     : http://localhost:8080"
echo
echo "Etat des conteneurs : docker compose ps"
echo "(le broker passe 'healthy' en ~20-30s ; l'UI demarre ensuite)"
