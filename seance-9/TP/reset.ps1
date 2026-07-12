# Arrete le cluster ET efface toutes les donnees (volumes).
# Pense-bete : Streams stocke aussi un etat local dans %TEMP%\kafka-streams\<application.id>.
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot
docker compose down -v
Write-Host "Cluster arrete et donnees effacees."
Write-Host "Pour repartir d'un etat Streams propre : Remove-Item -Recurse -Force $env:TEMP\kafka-streams\tp9-order-stream"
