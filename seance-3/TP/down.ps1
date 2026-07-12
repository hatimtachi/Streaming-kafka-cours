# Arrete le cluster. Les donnees des topics sont CONSERVEES (volumes).
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

docker compose down

Write-Host "Cluster arrete. Les donnees sont conservees."
Write-Host "Pour tout effacer et repartir a neuf : .\reset.ps1"
