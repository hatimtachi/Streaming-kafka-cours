# Demarre le cluster Kafka a 3 brokers (KRaft) + Kafka UI.
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot
docker compose up -d
Write-Host ""
Write-Host "Cluster (3 brokers) en cours de demarrage..."
Write-Host "  - Brokers (CLI hote / code Java) : localhost:29092 / 29093 / 29094"
Write-Host "  - Kafka UI                       : http://localhost:8080"
Write-Host ""
Write-Host "Etat des conteneurs : docker compose ps"
