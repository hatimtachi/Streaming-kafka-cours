# Affiche EN BOUCLE l'etat des partitions d'un topic (leaders + ISR).
# Lancez-le dans un terminal, puis coupez un broker dans un AUTRE terminal
# (docker compose stop broker-2) pour voir l'ISR et le leader bouger en direct.
#
# Usage : .\watch-isr.ps1 [topic]      (defaut : commandes)
$ErrorActionPreference = "Continue"
Set-Location -Path $PSScriptRoot
$topic = if ($args.Count -ge 1) { $args[0] } else { "commandes" }

while ($true) {
    Clear-Host
    Write-Host "ISR de '$topic'  (Ctrl-C pour quitter)  -  $(Get-Date -Format HH:mm:ss)"
    Write-Host "-----------------------------------------------------------"
    docker compose exec -T broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:29092 --describe --topic $topic
    Start-Sleep -Seconds 2
}
