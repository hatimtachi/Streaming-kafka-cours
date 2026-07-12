#!/usr/bin/env bash
# Demarre le cluster (3 brokers KRaft) et cree les topics du TP.
set -euo pipefail
cd "$(dirname "$0")"
echo "== Cluster Kafka : demarrage (3 brokers KRaft) =="
docker compose up -d --wait
echo "== Topics : creation =="
for t in orders-ts windowed-counts; do
  docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-1:19092 \
    --create --if-not-exists --topic "$t" --partitions 3 --replication-factor 3 >/dev/null
  echo "  ok : $t"
done
echo "== Pret. Bootstrap depuis l'hote : localhost:29092 =="
