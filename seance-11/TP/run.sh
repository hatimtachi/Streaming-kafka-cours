#!/usr/bin/env bash
# Compile et lance une appli de l'atelier (sur votre machine, via Maven).
#
# Usage : ./run.sh <app|seed> [solution]
#   ./run.sh app             -> fr.esgi.kafka.tp11.CurrencyAggregator   (votre agregation ; tourne en continu)
#   ./run.sh app solution    -> fr.esgi.kafka.tp11.solution.CurrencyAggregator (corrige)
#   ./run.sh seed            -> fr.esgi.kafka.tp11.OrderProducer        (envoie des commandes de demo)
#
# Demo de tolerance aux pannes : lancez "app", "seed", observez les totaux,
# arretez l'app (Ctrl-C), relancez "app" -> les totaux repartent de leur derniere
# valeur (restauration depuis le changelog), PAS de zero.
set -euo pipefail
cd "$(dirname "$0")"

base="fr.esgi.kafka.tp11"
what="${1:-}"
pkg="$base"
[ "${2:-}" = "solution" ] && pkg="$base.solution"

case "$what" in
  app)  main="$pkg.CurrencyAggregator" ;;
  seed) main="$base.OrderProducer" ;;     # toujours fourni
  *) echo "Usage : ./run.sh <app|seed> [solution]" >&2; exit 1 ;;
esac

mvn -q compile exec:java -Dexec.mainClass="$main"
