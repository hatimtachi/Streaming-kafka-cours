#!/usr/bin/env bash
# Lance un outil CLI Kafka A L'INTERIEUR du conteneur broker.
#
# Exemples :
#   ./kcli.sh kafka-topics.sh --bootstrap-server localhost:9092 --list
#   ./kcli.sh kafka-console-producer.sh --bootstrap-server localhost:9092 --topic premier-topic
#   ./kcli.sh kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic premier-topic --from-beginning
#
# Astuce : si Kafka est installe sur votre machine, vous pouvez lancer les memes
# commandes directement (sans ./kcli.sh) : le broker ecoute aussi sur localhost:9092.
set -euo pipefail
cd "$(dirname "$0")"

if [ "$#" -eq 0 ]; then
  echo "Usage : ./kcli.sh <outil-kafka.sh> [arguments...]" >&2
  echo "Exemple : ./kcli.sh kafka-topics.sh --bootstrap-server localhost:9092 --list" >&2
  exit 1
fi

tool="$1"; shift
exec docker compose exec broker /opt/kafka/bin/"$tool" "$@"
