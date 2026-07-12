# Arrete tout ET efface les donnees (brokers + schemas enregistres).
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot
docker compose down -v
Write-Host "Arrete et donnees effacees (y compris les schemas du registre)."
