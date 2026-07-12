#!/usr/bin/env bash
# Demarre le cluster Kafka a 3 brokers (KRaft) + Kafka UI.
set -euo pipefail
cd "$(dirname "$0")"

docker compose up -d

echo
echo "Cluster (3 brokers) en cours de demarrage..."
echo "  - Brokers (CLI hote) : localhost:9092 / 9094 / 9096"
echo "  - Kafka UI           : http://localhost:8080"
echo
echo "Etat des conteneurs : docker compose ps"
echo "(les 3 brokers passent 'healthy' en ~25-40s ; l'UI demarre ensuite)"
