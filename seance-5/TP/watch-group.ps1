# Affiche EN BOUCLE l'assignation et le LAG d'un groupe de consommateurs.
# Lancez-le pendant que vous ajoutez/coupez des consumers pour voir le rebalance.
#
# Usage : .\watch-group.ps1 [group.id]      (defaut : atelier-s5)
$ErrorActionPreference = "Continue"
Set-Location -Path $PSScriptRoot
$group = if ($args.Count -ge 1) { $args[0] } else { "atelier-s5" }

while ($true) {
    Clear-Host
    Write-Host "Groupe '$group'  -  assignation & LAG  (Ctrl-C pour quitter)  -  $(Get-Date -Format HH:mm:ss)"
    Write-Host "-------------------------------------------------------------------------"
    docker compose exec -T broker-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker-1:9092 --describe --group $group
    Start-Sleep -Seconds 2
}
