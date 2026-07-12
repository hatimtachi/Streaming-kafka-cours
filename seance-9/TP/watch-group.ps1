# Affiche en boucle la repartition des partitions du consumer group de l'application
# Streams (application.id = tp9-order-stream). Ctrl-C pour arreter.
$ErrorActionPreference = "Continue"
Set-Location -Path $PSScriptRoot
$group = if ($args.Count -ge 1) { $args[0] } else { "tp9-order-stream" }
Write-Host "Surveillance du groupe '$group' (Ctrl-C pour arreter)..."
while ($true) {
    Clear-Host
    Write-Host "== Groupe : $group ==   ($(Get-Date -Format HH:mm:ss))"
    docker compose exec -T broker-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker-1:9092 --describe --group $group
    Start-Sleep -Seconds 3
}
