#!/usr/bin/env bash
# Affiche EN BOUCLE l'etat des partitions d'un topic (leaders + ISR).
# Lancez-le dans un terminal, puis coupez un broker dans un AUTRE terminal
# (docker compose stop broker-2) pour voir l'ISR et le leader bouger en direct.
#
# Usage : ./watch-isr.sh [topic]      (defaut : commandes)
set -uo pipefail
cd "$(dirname "$0")"
topic="${1:-commandes}"

while true; do
  clear
  echo "ISR de '$topic'  (Ctrl-C pour quitter)  -  $(date +%H:%M:%S)"
  echo "-----------------------------------------------------------"
  docker compose exec -T broker-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server broker-1:29092 --describe --topic "$topic" 2>/dev/null \
    || echo "(en attente du cluster...)"
  sleep 2
done
