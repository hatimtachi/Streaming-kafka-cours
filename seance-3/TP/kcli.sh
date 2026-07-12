#!/usr/bin/env bash
# Lance un outil CLI Kafka A L'INTERIEUR du conteneur broker-1.
#
# A l'interieur du reseau Docker, les 3 brokers s'adressent par broker-N:29092.
# C'est cette adresse qu'il faut donner en --bootstrap-server.
#
# Exemples :
#   ./kcli.sh kafka-topics.sh --bootstrap-server broker-1:29092 --describe --topic commandes
#   ./kcli.sh kafka-console-producer.sh --bootstrap-server broker-1:29092 --topic commandes --producer-property acks=all
set -euo pipefail
cd "$(dirname "$0")"

if [ "$#" -eq 0 ]; then
  echo "Usage : ./kcli.sh <outil-kafka.sh> [arguments...]" >&2
  echo "Exemple : ./kcli.sh kafka-topics.sh --bootstrap-server broker-1:29092 --describe --topic commandes" >&2
  exit 1
fi

tool="$1"; shift
exec docker compose exec broker-1 /opt/kafka/bin/"$tool" "$@"
