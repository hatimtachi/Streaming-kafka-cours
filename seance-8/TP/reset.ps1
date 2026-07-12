# Arrete tout ET efface les donnees Kafka (volumes). Le dossier data/ reste sur disque.
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot
docker compose down -v
Write-Host "Arrete et donnees Kafka effacees."
Write-Host "Pour repartir d'un fichier source propre :"
Write-Host '  "o-1;19.9`no-2;5.0`no-3;42.0`n" | Set-Content data\orders.txt ; Remove-Item -ErrorAction SilentlyContinue data\orders-out.txt, data\orders-avro-out.txt'
