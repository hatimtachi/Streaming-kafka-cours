#!/usr/bin/env bash
# Demarre le cluster Kafka a 3 brokers (KRaft) + Schema Registry + Kafka UI.
set -euo pipefail
cd "$(dirname "$0")"
docker compose up -d
echo
echo "Demarrage en cours..."
echo "  - Brokers (CLI hote / code Java) : localhost:29092 / 29093 / 29094"
echo "  - Schema Registry (REST)         : http://localhost:8081"
echo "  - Kafka UI                       : http://localhost:8080"
echo
echo "Le registre met quelques secondes a repondre. Verifier :"
echo "  curl -s localhost:8081/subjects      (doit renvoyer [] au depart)"
