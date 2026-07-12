# Arrete le cluster ET efface toutes les donnees (volume). Repart de zero.
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

docker compose down -v

Write-Host "Cluster arrete et donnees effacees."
Write-Host ".\up.ps1 (ou 'docker compose up -d') repartira sur un cluster vierge."
