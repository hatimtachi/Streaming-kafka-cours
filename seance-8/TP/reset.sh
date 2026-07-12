#!/usr/bin/env bash
# Arrete tout ET efface les donnees Kafka (volumes). Le dossier data/ reste sur disque.
set -euo pipefail
cd "$(dirname "$0")"
docker compose down -v
echo "Arrete et donnees Kafka effacees."
echo "Pour repartir d'un fichier source propre :"
echo "  printf 'o-1;19.9\\no-2;5.0\\no-3;42.0\\n' > data/orders.txt ; rm -f data/orders-out.txt data/orders-avro-out.txt"
