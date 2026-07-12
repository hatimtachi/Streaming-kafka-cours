#!/usr/bin/env bash
# Compile et lance une appli de l'atelier (sur votre machine, via Maven).
#
# Usage : ./run.sh <producer|consumer> [solution]
#   ./run.sh producer            -> fr.esgi.kafka.tp5.EventProducer   (votre code)
#   ./run.sh consumer            -> fr.esgi.kafka.tp5.GroupConsumer
#   ./run.sh consumer solution   -> fr.esgi.kafka.tp5.solution.GroupConsumer (corrige)
#
# Pour le rebalance : lancez "./run.sh consumer" dans 2 ou 3 terminaux (meme group.id).
set -euo pipefail
cd "$(dirname "$0")"

base="fr.esgi.kafka.tp5"
what="${1:-}"
pkg="$base"
[ "${2:-}" = "solution" ] && pkg="$base.solution"

case "$what" in
  producer) main="$pkg.EventProducer" ;;
  consumer) main="$pkg.GroupConsumer" ;;
  *) echo "Usage : ./run.sh <producer|consumer> [solution]" >&2; exit 1 ;;
esac

mvn -q compile exec:java -Dexec.mainClass="$main"
