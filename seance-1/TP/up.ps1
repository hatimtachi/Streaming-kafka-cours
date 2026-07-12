# Demarre le cluster Kafka (1 broker KRaft) + Kafka UI.
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

docker compose up -d

Write-Host ""
Write-Host "Cluster en cours de demarrage..."
Write-Host "  - Broker (CLI) : localhost:9092"
Write-Host "  - Kafka UI     : http://localhost:8080"
Write-Host ""
Write-Host "Etat des conteneurs : docker compose ps"
Write-Host "(le broker passe 'healthy' en ~20-30s ; l'UI demarre ensuite)"
