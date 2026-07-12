#!/usr/bin/env bash
# Compile et lance une appli de l'atelier (sur votre machine, via Maven).
#
# Usage : ./run.sh <producer|consumer> [solution]
#   ./run.sh producer            -> app.ProducerApp   (votre code)
#   ./run.sh consumer            -> app.ConsumerApp
#   ./run.sh producer solution   -> solution.ProducerApp (corrige)
set -euo pipefail
cd "$(dirname "$0")"

what="${1:-}"
pkg="${2:-app}"
case "$what" in
  producer) main="$pkg.ProducerApp" ;;
  consumer) main="$pkg.ConsumerApp" ;;
  *) echo "Usage : ./run.sh <producer|consumer> [solution]" >&2; exit 1 ;;
esac

mvn -q compile exec:java -Dexec.mainClass="$main"
