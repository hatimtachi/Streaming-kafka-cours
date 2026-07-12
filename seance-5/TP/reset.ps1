# Arrete le cluster ET efface toutes les donnees (volumes).
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot
docker compose down -v
Write-Host "Cluster arrete et donnees effacees."
