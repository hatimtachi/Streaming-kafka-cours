#!/usr/bin/env bash
# Affiche EN BOUCLE l'assignation et le LAG d'un groupe de consommateurs.
# Lancez-le dans un terminal pendant que vous ajoutez/coupez des consumers :
# vous voyez le rebalance (CONSUMER-ID par partition) et le LAG evoluer.
#
# Usage : ./watch-group.sh [group.id]      (defaut : atelier-s5)
set -uo pipefail
cd "$(dirname "$0")"
group="${1:-atelier-s5}"

while true; do
  clear
  echo "Groupe '$group'  -  assignation & LAG  (Ctrl-C pour quitter)  -  $(date +%H:%M:%S)"
  echo "-------------------------------------------------------------------------"
  docker compose exec -T broker-1 /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server broker-1:9092 --describe --group "$group" 2>/dev/null \
    || echo "(groupe pas encore actif, ou rebalance en cours...)"
  sleep 2
done
