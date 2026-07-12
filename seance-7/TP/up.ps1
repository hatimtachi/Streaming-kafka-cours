# Demarre le cluster Kafka a 3 brokers (KRaft) + Schema Registry + Kafka UI.
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot
docker compose up -d
Write-Host ""
Write-Host "Demarrage en cours..."
Write-Host "  - Brokers (CLI hote / code Java) : localhost:29092 / 29093 / 29094"
Write-Host "  - Schema Registry (REST)         : http://localhost:8081"
Write-Host "  - Kafka UI                       : http://localhost:8080"
Write-Host ""
Write-Host "Le registre met quelques secondes a repondre. Verifier :"
Write-Host "  curl.exe -s localhost:8081/subjects   (doit renvoyer [] au depart)"
