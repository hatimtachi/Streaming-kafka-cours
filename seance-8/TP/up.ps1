# Demarre la pile : 3 brokers (KRaft) + Schema Registry + Kafka Connect + Kafka UI.
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot
docker compose up -d
Write-Host ""
Write-Host "Demarrage en cours..."
Write-Host "  - Brokers (CLI hote)     : localhost:29092 / 29093 / 29094"
Write-Host "  - Schema Registry (REST) : http://localhost:8081"
Write-Host "  - Kafka Connect (REST)   : http://localhost:8083"
Write-Host "  - Kafka UI               : http://localhost:8080"
Write-Host ""
Write-Host "Le worker Connect met ~30-60 s a demarrer (scan des plugins). Verifier :"
Write-Host "  curl.exe -s localhost:8083/             (renvoie la version)"
Write-Host "  .\connect.ps1 plugins                   (doit lister FileStreamSource/Sink)"
