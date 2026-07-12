# Arrete le cluster.  Passez "clean" pour effacer aussi l'etat (volumes).
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
if ($args.Count -ge 1 -and $args[0] -eq "clean") {
  Write-Host "== Cluster : arret + purge des volumes (etat efface) =="
  docker compose down -v
} else {
  Write-Host "== Cluster : arret (etat conserve) =="
  docker compose down
}
