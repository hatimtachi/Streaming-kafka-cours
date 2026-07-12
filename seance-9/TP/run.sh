#!/usr/bin/env bash
# Compile et lance une appli de l'atelier (sur votre machine, via Maven).
#
# Usage : ./run.sh <app|seed> [solution]
#   ./run.sh app             -> fr.esgi.kafka.tp9.OrderStreamApp   (votre topologie ; tourne en continu)
#   ./run.sh app solution    -> fr.esgi.kafka.tp9.solution.OrderStreamApp (corrige)
#   ./run.sh seed            -> fr.esgi.kafka.tp9.OrderProducer    (envoie des commandes de demo)
#
# Astuce parallelisme : lancez "./run.sh app" dans DEUX terminaux (meme application.id)
# pour voir les 3 partitions se repartir entre les instances.
set -euo pipefail
cd "$(dirname "$0")"

base="fr.esgi.kafka.tp9"
what="${1:-}"
pkg="$base"
[ "${2:-}" = "solution" ] && pkg="$base.solution"

case "$what" in
  app)  main="$pkg.OrderStreamApp" ;;
  seed) main="$base.OrderProducer" ;;     # toujours fourni (pas de variante solution)
  *) echo "Usage : ./run.sh <app|seed> [solution]" >&2; exit 1 ;;
esac

mvn -q compile exec:java -Dexec.mainClass="$main"
