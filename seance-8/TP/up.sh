#!/usr/bin/env bash
# Demarre la pile : 3 brokers (KRaft) + Schema Registry + Kafka Connect + Kafka UI.
set -euo pipefail
cd "$(dirname "$0")"
docker compose up -d
echo
echo "Demarrage en cours..."
echo "  - Brokers (CLI hote)     : localhost:29092 / 29093 / 29094"
echo "  - Schema Registry (REST) : http://localhost:8081"
echo "  - Kafka Connect (REST)   : http://localhost:8083"
echo "  - Kafka UI               : http://localhost:8080"
echo
echo "Le worker Connect met ~30-60 s a demarrer (scan des plugins). Verifier :"
echo "  curl -s localhost:8083/                 (renvoie la version)"
echo "  ./connect.sh plugins                    (doit lister FileStreamSource/Sink)"
