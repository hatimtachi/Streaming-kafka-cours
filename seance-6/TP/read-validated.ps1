# Lit le topic de sortie "orders-validated" avec un niveau d'isolation donne.
#
# Usage : .\read-validated.ps1 [read_committed|read_uncommitted]   (defaut : read_committed)
$ErrorActionPreference = "Continue"
Set-Location -Path $PSScriptRoot
$level = if ($args.Count -ge 1) { $args[0] } else { "read_committed" }

Write-Host "Lecture de 'orders-validated' en $level (timeout 4s)..."
docker compose exec -T broker-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker-1:9092 --topic orders-validated --from-beginning --isolation-level $level --timeout-ms 4000 --property print.key=true
