# Arrete toute la pile. Donnees CONSERVEES (volumes + dossier data/).
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot
docker compose down
Write-Host "Arrete. Donnees conservees. Tout effacer : .\reset.ps1"
