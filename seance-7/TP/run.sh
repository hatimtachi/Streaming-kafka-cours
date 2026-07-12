#!/usr/bin/env bash
# Compile et lance une appli Avro de l'atelier (sur votre machine, via Maven).
#
# Usage : ./run.sh <producer|consumer> [solution] [v1|v2]
#   ./run.sh producer              -> producteur Avro, schema v1 (votre code)
#   ./run.sh producer v2           -> producteur Avro, schema v2
#   ./run.sh consumer              -> consommateur Avro (votre code)
#   ./run.sh producer solution v2  -> corrige, schema v2
#   ./run.sh consumer solution     -> corrige
set -euo pipefail
cd "$(dirname "$0")"

base="fr.esgi.kafka.tp7"
role="${1:-}"; shift || true
pkg="$base"
if [ "${1:-}" = "solution" ]; then pkg="$base.solution"; shift || true; fi
version="${1:-}"          # v1 / v2 (producteur uniquement)

case "$role" in
  producer) main="$pkg.AvroOrderProducer" ;;
  consumer) main="$pkg.AvroOrderConsumer" ;;
  *) echo "Usage : ./run.sh <producer|consumer> [solution] [v1|v2]" >&2; exit 1 ;;
esac

if [ -n "$version" ]; then
  mvn -q compile exec:java -Dexec.mainClass="$main" -Dexec.args="$version"
else
  mvn -q compile exec:java -Dexec.mainClass="$main"
fi
