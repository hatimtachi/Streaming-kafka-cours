# Arrete le cluster + le registre. Les donnees sont CONSERVEES (volumes).
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot
docker compose down
Write-Host "Arrete. Donnees conservees. Tout effacer : .\reset.ps1"
