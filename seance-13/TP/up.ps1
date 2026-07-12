#Requires -Version 5.1
# Demarre le cluster (3 brokers KRaft) et cree les topics du TP.
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
Write-Host "== Cluster Kafka : demarrage (3 brokers KRaft) =="
docker compose up -d --wait
Write-Host "== Topics : creation =="
$topics = @("orders-ts","windowed-counts")
foreach ($t in $topics) {
  docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server kafka-1:19092 `
    --create --if-not-exists --topic $t --partitions 3 --replication-factor 3 | Out-Null
  Write-Host "  ok : $t"
}
Write-Host "== Pret. Bootstrap depuis l'hote : localhost:29092 =="
