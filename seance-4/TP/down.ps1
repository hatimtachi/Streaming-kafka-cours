# Arrete le cluster. Les donnees des topics sont CONSERVEES (volumes).
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot
docker compose down
Write-Host "Cluster arrete. Donnees conservees. Tout effacer : .\reset.ps1"
