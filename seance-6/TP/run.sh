#!/usr/bin/env bash
# Compile et lance une appli de l'atelier (sur votre machine, via Maven).
#
# Usage : ./run.sh <seed|pipeline> [solution]
#   ./run.sh seed                -> fr.esgi.kafka.tp6.OrderSeeder   (alimente "orders", fourni)
#   ./run.sh pipeline            -> fr.esgi.kafka.tp6.EosPipeline   (votre code)
#   ./run.sh pipeline solution   -> fr.esgi.kafka.tp6.solution.EosPipeline (corrige)
set -euo pipefail
cd "$(dirname "$0")"

base="fr.esgi.kafka.tp6"
what="${1:-}"
pkg="$base"
[ "${2:-}" = "solution" ] && pkg="$base.solution"

case "$what" in
  seed)     main="$base.OrderSeeder" ;;      # toujours fourni (pas de variante solution)
  pipeline) main="$pkg.EosPipeline" ;;
  *) echo "Usage : ./run.sh <seed|pipeline> [solution]" >&2; exit 1 ;;
esac

mvn -q compile exec:java -Dexec.mainClass="$main"
