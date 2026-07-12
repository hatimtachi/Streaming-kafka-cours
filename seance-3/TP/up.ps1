# Demarre le cluster Kafka a 3 brokers (KRaft) + Kafka UI.
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

docker compose up -d

Write-Host ""
Write-Host "Cluster (3 brokers) en cours de demarrage..."
Write-Host "  - Brokers (CLI hote) : localhost:9092 / 9094 / 9096"
Write-Host "  - Kafka UI           : http://localhost:8080"
Write-Host ""
Write-Host "Etat des conteneurs : docker compose ps"
Write-Host "(les 3 brokers passent 'healthy' en ~25-40s ; l'UI demarre ensuite)"
